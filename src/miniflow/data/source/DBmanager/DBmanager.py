# data/source/dbsystem.py
import asyncio
import asyncpg
import aiomysql
import aiosqlite
from typing import Dict, Any
from dataclasses import dataclass
import logging

from .base import Source

logger = logging.getLogger(__name__)

@dataclass
class DBConfig:
    """데이터베이스 설정"""
    db_type: str  # 'postgresql', 'mysql', 'sqlite'
    host: str = 'localhost'
    port: int = 5432
    database: str = ''
    username: str = ''
    password: str = ''
    # 연결 풀 설정
    min_size: int = 5
    max_size: int = 20
    max_queries: int = 50000
    max_inactive_connection_lifetime: float = 300.0

class ConnectionPoolManager:
    """데이터베이스 연결 풀 관리자"""
    _pools: Dict[str, Any] = {}
    _pool_locks: Dict[str, asyncio.Lock] = {}
    _main_lock = asyncio.Lock()
    
    @classmethod
    async def get_pool(cls, config: DBConfig):
        """연결 풀 가져오기 또는 생성"""
        pool_key = cls._generate_pool_key(config)
        
        # 풀별 락 관리
        async with cls._main_lock:
            if pool_key not in cls._pool_locks:
                cls._pool_locks[pool_key] = asyncio.Lock()
        
        # 풀별 락으로 동시 생성 방지
        async with cls._pool_locks[pool_key]:
            if pool_key not in cls._pools:
                logger.info(f"Creating new connection pool: {pool_key}")
                pool = await cls._create_pool(config)
                cls._pools[pool_key] = pool
            else:
                logger.debug(f"Reusing existing pool: {pool_key}")
            
            return cls._pools[pool_key]
    
    @classmethod
    async def _create_pool(cls, config: DBConfig):
        """실제 연결 풀 생성"""
        if config.db_type == 'postgresql':
            return await asyncpg.create_pool(
                host=config.host,
                port=config.port,
                user=config.username,
                password=config.password,
                database=config.database,
                min_size=config.min_size,
                max_size=config.max_size,
                max_queries=config.max_queries,
                max_inactive_connection_lifetime=config.max_inactive_connection_lifetime
            )
        elif config.db_type == 'mysql':
            return await aiomysql.create_pool(
                host=config.host,
                port=config.port,
                user=config.username,
                password=config.password,
                db=config.database,
                minsize=config.min_size,
                maxsize=config.max_size
            )
        elif config.db_type == 'sqlite':
            # SQLite는 연결 풀을 직접 지원하지 않으므로 세마포어 사용
            return {
                'database': config.database,
                'semaphore': asyncio.Semaphore(config.max_size)
            }
        else:
            raise ValueError(f"Unsupported database type: {config.db_type}")
    
    @classmethod
    def _generate_pool_key(cls, config: DBConfig) -> str:
        """연결 풀 키 생성"""
        return f"{config.db_type}://{config.username}@{config.host}:{config.port}/{config.database}"
    
    @classmethod
    async def close_pool(cls, config: DBConfig):
        """특정 연결 풀 종료"""
        pool_key = cls._generate_pool_key(config)
        if pool_key in cls._pools:
            pool = cls._pools[pool_key]
            if config.db_type in ['postgresql', 'mysql']:
                await pool.close()
            del cls._pools[pool_key]
            logger.info(f"Closed connection pool: {pool_key}")
    
    @classmethod
    async def close_all_pools(cls):
        """모든 연결 풀 종료"""
        for pool_key, pool in cls._pools.items():
            try:
                if hasattr(pool, 'close'):
                    await pool.close()
                logger.info(f"Closed pool: {pool_key}")
            except Exception as e:
                logger.error(f"Error closing pool {pool_key}: {e}")
        
        cls._pools.clear()
        cls._pool_locks.clear()

class SourceDB(Source):
    """업그레이드된 데이터베이스 소스 (연결 풀 지원)"""
    
    # 전역 DB 접근 제한 (모든 DB 타입 통합)
    _global_db_semaphore = asyncio.Semaphore(50)
    
    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.config = DBConfig(**params.get('db_config', {}))
        self.query = params.get('query', '')
        self.query_params = params.get('query_params', {})
    
    async def __call__(self, data=None) -> Any:
        """데이터베이스 쿼리 실행"""
        async with self._global_db_semaphore:
            pool = await ConnectionPoolManager.get_pool(self.config)
            
            if self.config.db_type == 'postgresql':
                return await self._execute_postgresql(pool)
            elif self.config.db_type == 'mysql':
                return await self._execute_mysql(pool)
            elif self.config.db_type == 'sqlite':
                return await self._execute_sqlite(pool)
            else:
                raise ValueError(f"Unsupported database type: {self.config.db_type}")
    
    async def _execute_postgresql(self, pool):
        """PostgreSQL 쿼리 실행"""
        async with pool.acquire() as connection:
            logger.debug(f"Executing PostgreSQL query: {self.query[:100]}...")
            
            if self.query_params:
                result = await connection.fetch(self.query, *self.query_params.values())
            else:
                result = await connection.fetch(self.query)
            
            # Record 객체를 딕셔너리로 변환
            return [dict(record) for record in result]
    
    async def _execute_mysql(self, pool):
        """MySQL 쿼리 실행"""
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                logger.debug(f"Executing MySQL query: {self.query[:100]}...")
                
                if self.query_params:
                    await cursor.execute(self.query, self.query_params)
                else:
                    await cursor.execute(self.query)
                
                result = await cursor.fetchall()
                
                # 컬럼명과 함께 딕셔너리로 변환
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in result]
    
    async def _execute_sqlite(self, pool_info):
        """SQLite 쿼리 실행"""
        async with pool_info['semaphore']:
            async with aiosqlite.connect(pool_info['database']) as connection:
                logger.debug(f"Executing SQLite query: {self.query[:100]}...")
                
                connection.row_factory = aiosqlite.Row  # 딕셔너리 형태로 결과 반환
                
                if self.query_params:
                    cursor = await connection.execute(self.query, self.query_params)
                else:
                    cursor = await connection.execute(self.query)
                
                result = await cursor.fetchall()
                return [dict(row) for row in result]
    
    @staticmethod
    async def close_connections():
        """모든 데이터베이스 연결 정리"""
        await ConnectionPoolManager.close_all_pools()
    
    @classmethod
    def get_pool_stats(cls) -> Dict[str, Any]:
        """연결 풀 통계 정보"""
        stats = {}
        for pool_key, pool in ConnectionPoolManager._pools.items():
            if hasattr(pool, 'get_size'):
                stats[pool_key] = {
                    "size": pool.get_size(),
                    "max_size": pool.get_max_size(),
                    "min_size": pool.get_min_size()
                }
            else:
                stats[pool_key] = {"type": "sqlite_semaphore"}
        
        return stats

# 사용 예제
"""
# PostgreSQL 사용
db_source = SourceDB(params={
    'db_config': {
        'db_type': 'postgresql',
        'host': 'localhost',
        'port': 5432,
        'database': 'mydb',
        'username': 'user',
        'password': 'pass',
        'min_size': 5,
        'max_size': 20
    },
    'query': 'SELECT * FROM users WHERE age > $1',
    'query_params': {'age': 25}
})

# 데이터 노드와 함께 사용
user_node = CommonNode(
    element="users_over_25",
    process=db_source
)

result = await user_node.get_data()
"""