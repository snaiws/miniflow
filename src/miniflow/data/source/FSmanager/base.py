# data/source/storage/base.py
from abc import ABC, abstractmethod
import asyncio
from typing import Dict, Any, Optional, Union, List, AsyncIterator
from dataclasses import dataclass
from enum import Enum
import logging

from ..base import Source

logger = logging.getLogger(__name__)

class StorageOperation(Enum):
    """스토리지 작업 타입"""
    READ = "read"
    WRITE = "write"
    LIST = "list"
    DELETE = "delete"
    EXISTS = "exists"
    COPY = "copy"
    MOVE = "move"
    GET_METADATA = "get_metadata"

class CompressionType(Enum):
    """압축 타입"""
    NONE = "none"
    GZIP = "gzip"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    BROTLI = "brotli"

@dataclass
class StorageMetadata:
    """스토리지 메타데이터"""
    size: Optional[int] = None
    created_time: Optional[str] = None
    modified_time: Optional[str] = None
    content_type: Optional[str] = None
    etag: Optional[str] = None
    custom_metadata: Optional[Dict[str, str]] = None

@dataclass
class StorageConfig:
    """스토리지 기본 설정"""
    # 연결 설정
    connection_timeout: float = 30.0
    read_timeout: float = 60.0
    retry_attempts: int = 3
    retry_delay: float = 1.0
    
    # 성능 설정
    chunk_size: int = 8192  # 8KB
    max_concurrent_ops: int = 10
    
    # 압축 설정
    compression: CompressionType = CompressionType.NONE
    
    # 보안 설정
    ssl_verify: bool = True
    
    # 캐싱 설정
    enable_caching: bool = True
    cache_ttl: int = 3600  # 1시간

class BaseStorageSource(Source):
    """모든 스토리지 소스의 기본 클래스"""
    
    # 전역 동시성 제어
    _global_semaphore = asyncio.Semaphore(50)
    _operation_locks: Dict[str, asyncio.Lock] = {}
    _main_lock = asyncio.Lock()
    
    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.config = self._create_config(params.get('storage_config', {}))
        self.operation = StorageOperation(params.get('operation', 'read'))
        self.path = params.get('path', '')
        self.data = params.get('data', None)
        
        # 추가 옵션들
        self.recursive = params.get('recursive', False)
        self.pattern = params.get('pattern', '*')
        self.metadata_only = params.get('metadata_only', False)
    
    def _create_config(self, config_params: Dict[str, Any]) -> StorageConfig:
        """설정 객체 생성 (하위 클래스에서 오버라이드 가능)"""
        return StorageConfig(**config_params)
    
    async def __call__(self, data: Any = None) -> Any:
        """스토리지 작업 실행"""
        # 입력 데이터가 있으면 쓰기 작업으로 설정
        if data is not None:
            self.data = data
            self.operation = StorageOperation.WRITE
        
        return await self._execute_with_retry()
    
    async def _execute_with_retry(self) -> Any:
        """재시도 로직 포함 실행"""
        last_exception = None
        
        for attempt in range(self.config.retry_attempts):
            try:
                return await self._execute_operation()
            except Exception as e:
                last_exception = e
                if attempt < self.config.retry_attempts - 1:
                    wait_time = self.config.retry_delay * (2 ** attempt)  # 지수 백오프
                    logger.warning(f"Attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"All {self.config.retry_attempts} attempts failed")
        
        raise last_exception
    
    async def _execute_operation(self) -> Any:
        """동시성 제어 포함 작업 실행"""
        async with self._global_semaphore:
            # 경로별 락 관리
            async with self._main_lock:
                if self.path not in self._operation_locks:
                    self._operation_locks[self.path] = asyncio.Lock()
            
            # 쓰기 작업은 락 사용, 읽기 작업은 병렬 허용
            if self.operation in [StorageOperation.WRITE, StorageOperation.DELETE, 
                                 StorageOperation.MOVE, StorageOperation.COPY]:
                async with self._operation_locks[self.path]:
                    return await self._do_operation()
            else:
                return await self._do_operation()
    
    async def _do_operation(self) -> Any:
        """실제 작업 수행"""
        if self.operation == StorageOperation.READ:
            return await self.read(self.path)
        elif self.operation == StorageOperation.WRITE:
            return await self.write(self.path, self.data)
        elif self.operation == StorageOperation.LIST:
            return await self.list_objects(self.path, self.pattern, self.recursive)
        elif self.operation == StorageOperation.DELETE:
            return await self.delete(self.path)
        elif self.operation == StorageOperation.EXISTS:
            return await self.exists(self.path)
        elif self.operation == StorageOperation.GET_METADATA:
            return await self.get_metadata(self.path)
        else:
            raise ValueError(f"Unsupported operation: {self.operation}")
    
    # === 하위 클래스에서 구현해야 하는 추상 메서드들 ===
    
    @abstractmethod
    async def read(self, path: str) -> Any:
        """파일/객체 읽기"""
        pass
    
    @abstractmethod
    async def write(self, path: str, data: Any) -> bool:
        """파일/객체 쓰기"""
        pass
    
    @abstractmethod
    async def list_objects(self, path: str = "", pattern: str = "*", 
                          recursive: bool = False) -> List[str]:
        """객체 목록 조회"""
        pass
    
    @abstractmethod
    async def delete(self, path: str) -> bool:
        """파일/객체 삭제"""
        pass
    
    @abstractmethod
    async def exists(self, path: str) -> bool:
        """파일/객체 존재 확인"""
        pass
    
    @abstractmethod
    async def get_metadata(self, path: str) -> StorageMetadata:
        """메타데이터 조회"""
        pass
    
    # === 선택적으로 구현 가능한 메서드들 ===
    
    async def copy(self, source_path: str, dest_path: str) -> bool:
        """객체 복사 (기본 구현: 읽고 쓰기)"""
        try:
            data = await self.read(source_path)
            return await self.write(dest_path, data)
        except Exception as e:
            logger.error(f"Error copying {source_path} to {dest_path}: {e}")
            return False
    
    async def move(self, source_path: str, dest_path: str) -> bool:
        """객체 이동 (기본 구현: 복사 후 삭제)"""
        try:
            if await self.copy(source_path, dest_path):
                return await self.delete(source_path)
            return False
        except Exception as e:
            logger.error(f"Error moving {source_path} to {dest_path}: {e}")
            return False
    
    async def read_stream(self, path: str) -> AsyncIterator[bytes]:
        """스트림 읽기 (대용량 파일용)"""
        # 기본 구현: 전체 읽기 후 청크로 분할
        data = await self.read(path)
        if isinstance(data, bytes):
            for i in range(0, len(data), self.config.chunk_size):
                yield data[i:i + self.config.chunk_size]
        else:
            yield str(data).encode()
    
    async def write_stream(self, path: str, data_stream: AsyncIterator[bytes]) -> bool:
        """스트림 쓰기 (대용량 파일용)"""
        # 기본 구현: 모든 청크를 모아서 한번에 쓰기
        chunks = []
        async for chunk in data_stream:
            chunks.append(chunk)
        
        combined_data = b''.join(chunks)
        return await self.write(path, combined_data)


# === 특화된 기본 클래스들 ===

class ObjectStorageSource(BaseStorageSource):
    """오브젝트 스토리지 (S3, GCS, Azure Blob 등) 기본 클래스"""
    
    @dataclass
    class ObjectStorageConfig(StorageConfig):
        # 오브젝트 스토리지 전용 설정
        bucket_name: str = ""
        endpoint_url: Optional[str] = None
        region: str = "us-east-1"
        access_key: str = ""
        secret_key: str = ""
        
        # 멀티파트 업로드 설정
        multipart_threshold: int = 64 * 1024 * 1024  # 64MB
        multipart_chunksize: int = 16 * 1024 * 1024  # 16MB
        max_concurrency: int = 10
    
    def _create_config(self, config_params: Dict[str, Any]) -> ObjectStorageConfig:
        return self.ObjectStorageConfig(**config_params)
    
    @abstractmethod
    async def create_bucket(self, bucket_name: str) -> bool:
        """버킷 생성"""
        pass
    
    @abstractmethod
    async def delete_bucket(self, bucket_name: str) -> bool:
        """버킷 삭제"""
        pass
    
    @abstractmethod
    async def list_buckets(self) -> List[str]:
        """버킷 목록 조회"""
        pass


class FileSystemSource(BaseStorageSource):
    """파일 시스템 (로컬, NFS, HDFS 등) 기본 클래스"""
    
    @dataclass
    class FileSystemConfig(StorageConfig):
        # 파일 시스템 전용 설정
        base_path: str = ""
        use_proxy: bool = True
        create_dirs: bool = True
        encoding: str = "utf-8"
        
        # 권한 설정
        file_mode: Optional[int] = None
        dir_mode: Optional[int] = None


class DatabaseStorageSource(BaseStorageSource):
    """데이터베이스 스토리지 (MongoDB, Cassandra 등) 기본 클래스"""
    
    @dataclass 
    class DatabaseStorageConfig(StorageConfig):
        # 데이터베이스 전용 설정
        host: str = "localhost"
        port: int = 27017
        database: str = ""
        collection: str = ""
        username: str = ""
        password: str = ""
        
        # 연결 풀 설정
        min_pool_size: int = 5
        max_pool_size: int = 20


# === 사용 예제 ===
"""
# S3 스타일 오브젝트 스토리지
class SourceS3(ObjectStorageSource):
    async def read(self, path: str) -> Any:
        # boto3를 사용한 S3 읽기 구현
        pass
    
    async def write(self, path: str, data: Any) -> bool:
        # boto3를 사용한 S3 쓰기 구현
        pass
    
    # ... 나머지 메서드들 구현

# HDFS 스타일 분산 파일 시스템
class SourceHDFS(FileSystemSource):
    async def read(self, path: str) -> Any:
        # hdfs3 또는 snakebite를 사용한 HDFS 읽기
        pass
    
    async def write(self, path: str, data: Any) -> bool:
        # HDFS 쓰기 구현
        pass
    
    # ... 나머지 메서드들 구현

# 사용법
s3_source = SourceS3(params={
    'storage_config': {
        'bucket_name': 'my-bucket',
        'access_key': 'xxx',
        'secret_key': 'yyy',
        'retry_attempts': 5
    },
    'operation': 'read',
    'path': 'data/users.csv'
})

hdfs_source = SourceHDFS(params={
    'storage_config': {
        'base_path': '/user/data',
        'create_dirs': True
    },
    'operation': 'write',
    'path': 'processed/results.parquet'
})
"""