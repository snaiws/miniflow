# data/source/storage/mongodb.py
import asyncio
import json
from typing import Any, List, Optional, Dict, AsyncIterator
from dataclasses import dataclass
from datetime import datetime
import logging

import motor.motor_asyncio
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError
from bson import ObjectId
import pandas as pd

from .base import DatabaseStorageSource, StorageMetadata

logger = logging.getLogger(__name__)

class SourceMongoDB(DatabaseStorageSource):
    """MongoDB 도큐먼트 데이터베이스 소스"""
    
    def __init__(self, params: dict):
        super().__init__(params)
        self._client = None
        self._database = None
        
        # MongoDB 전용 설정
        self.query = params.get('query', {})
        self.projection = params.get('projection', None)
        self.sort = params.get('sort', None)
        self.limit = params.get('limit', None)
        self.skip = params.get('skip', None)
    
    async def _get_client(self):
        """MongoDB 클라이언트 가져오기 (지연 초기화)"""
        if self._client is None:
            # 연결 문자열 생성
            if self.config.username and self.config.password:
                connection_string = f"mongodb://{self.config.username}:{self.config.password}@{self.config.host}:{self.config.port}/{self.config.database}"
            else:
                connection_string = f"mongodb://{self.config.host}:{self.config.port}/{self.config.database}"
            
            self._client = motor.motor_asyncio.AsyncIOMotorClient(
                connection_string,
                minPoolSize=self.config.min_pool_size,
                maxPoolSize=self.config.max_pool_size,
                connectTimeoutMS=int(self.config.connection_timeout * 1000),
                serverSelectionTimeoutMS=int(self.config.read_timeout * 1000),
                retryWrites=True,
                retryReads=True
            )
            
            # 연결 테스트
            try:
                await self._client.admin.command('ping')
                logger.debug(f"Connected to MongoDB: {self.config.host}:{self.config.port}")
            except ServerSelectionTimeoutError:
                logger.error(f"Failed to connect to MongoDB: {self.config.host}:{self.config.port}")
                raise
        
        return self._client
    
    async def _get_database(self):
        """MongoDB 데이터베이스 가져오기"""
        if self._database is None:
            client = await self._get_client()
            self._database = client[self.config.database]
        return self._database
    
    async def _get_collection(self):
        """MongoDB 컬렉션 가져오기"""
        database = await self._get_database()
        return database[self.config.collection]
    
    def _serialize_for_json(self, obj):
        """MongoDB 객체를 JSON 직렬화 가능하게 변환"""
        if isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {key: self._serialize_for_json(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._serialize_for_json(item) for item in obj]
        else:
            return obj
    
    async def read(self, path: str = "") -> Any:
        """MongoDB 도큐먼트 읽기/조회"""
        try:
            collection = await self._get_collection()
            
            logger.debug(f"Querying MongoDB collection: {self.config.collection}")
            logger.debug(f"Query: {self.query}")
            
            # 커서 생성
            cursor = collection.find(self.query, self.projection)
            
            # 정렬 적용
            if self.sort:
                cursor = cursor.sort(self.sort)
            
            # 페이징 적용
            if self.skip:
                cursor = cursor.skip(self.skip)
            if self.limit:
                cursor = cursor.limit(self.limit)
            
            # 결과 수집
            documents = []
            async for doc in cursor:
                # ObjectId 등을 직렬화 가능하게 변환
                serialized_doc = self._serialize_for_json(doc)
                documents.append(serialized_doc)
            
            logger.debug(f"Retrieved {len(documents)} documents from MongoDB")
            
            # 단일 도큐먼트인 경우 리스트가 아닌 객체로 반환
            if len(documents) == 1 and not isinstance(self.query.get('_id'), dict):
                return documents[0]
            
            return documents
            
        except PyMongoError as e:
            logger.error(f"MongoDB read error: {e}")
            raise
    
    async def write(self, path: str, data: Any) -> bool:
        """MongoDB 도큐먼트 쓰기/삽입"""
        try:
            collection = await self._get_collection()
            
            if isinstance(data, list):
                # 여러 도큐먼트 삽입
                if data:  # 빈 리스트가 아닌 경우만
                    result = await collection.insert_many(data)
                    logger.debug(f"Inserted {len(result.inserted_ids)} documents to MongoDB")
                    return True
                else:
                    logger.warning("Empty list provided for MongoDB insert")
                    return False
            elif isinstance(data, dict):
                # 단일 도큐먼트 삽입
                result = await collection.insert_one(data)
                logger.debug(f"Inserted document to MongoDB: {result.inserted_id}")
                return True
            elif isinstance(data, pd.DataFrame):
                # DataFrame을 도큐먼트 리스트로 변환
                documents = data.to_dict('records')
                if documents:
                    result = await collection.insert_many(documents)
                    logger.debug(f"Inserted {len(result.inserted_ids)} documents from DataFrame")
                    return True
                else:
                    logger.warning("Empty DataFrame provided for MongoDB insert")
                    return False
            else:
                # 기타 데이터 타입은 딕셔너리로 래핑
                doc = {'data': data, 'timestamp': datetime.utcnow()}
                result = await collection.insert_one(doc)
                logger.debug(f"Inserted wrapped document to MongoDB: {result.inserted_id}")
                return True
                
        except PyMongoError as e:
            logger.error(f"MongoDB write error: {e}")
            raise
    
    async def list_objects(self, path: str = "", pattern: str = "*", recursive: bool = False) -> List[str]:
        """MongoDB 컬렉션 목록 조회"""
        try:
            database = await self._get_database()
            
            # 컬렉션 목록 가져오기
            collections = await database.list_collection_names()
            
            # 패턴 매칭
            if pattern != "*":
                import fnmatch
                collections = [col for col in collections if fnmatch.fnmatch(col, pattern)]
            
            return collections
            
        except PyMongoError as e:
            logger.error(f"MongoDB list collections error: {e}")
            raise
    
    async def delete(self, path: str) -> bool:
        """MongoDB 도큐먼트 삭제"""
        try:
            collection = await self._get_collection()
            
            if self.query:
                # 쿼리에 매칭되는 도큐먼트들 삭제
                result = await collection.delete_many(self.query)
                deleted_count = result.deleted_count
                
                logger.debug(f"Deleted {deleted_count} documents from MongoDB")
                return deleted_count > 0
            else:
                # 특정 ID로 삭제 (path를 ObjectId로 간주)
                try:
                    object_id = ObjectId(path)
                    result = await collection.delete_one({'_id': object_id})
                    
                    logger.debug(f"Deleted document with ID: {path}")
                    return result.deleted_count > 0
                except:
                    # ObjectId가 아닌 경우, path를 쿼리 필드로 사용
                    result = await collection.delete_many({'name': path})
                    return result.deleted_count > 0
                    
        except PyMongoError as e:
            logger.error(f"MongoDB delete error: {e}")
            raise
    
    async def exists(self, path: str) -> bool:
        """MongoDB 도큐먼트 존재 확인"""
        try:
            collection = await self._get_collection()
            
            if self.query:
                # 쿼리로 존재 확인
                count = await collection.count_documents(self.query, limit=1)
                return count > 0
            else:
                # 특정 ID로 존재 확인
                try:
                    object_id = ObjectId(path)
                    doc = await collection.find_one({'_id': object_id})
                    return doc is not None
                except:
                    # ObjectId가 아닌 경우
                    doc = await collection.find_one({'name': path})
                    return doc is not None
                    
        except PyMongoError as e:
            logger.error(f"MongoDB exists check error: {e}")
            raise
    
    async def get_metadata(self, path: str) -> StorageMetadata:
        """MongoDB 컬렉션 메타데이터 조회"""
        try:
            database = await self._get_database()
            collection = await self._get_collection()
            
            # 컬렉션 통계
            stats = await database.command("collStats", self.config.collection)
            
            # 도큐먼트 수
            doc_count = await collection.count_documents({})
            
            return StorageMetadata(
                size=stats.get('size', 0),
                created_time=None,  # MongoDB는 컬렉션 생성 시간을 제공하지 않음
                modified_time=None,
                content_type='application/bson',
                etag=None,
                custom_metadata={
                    'document_count': doc_count,
                    'storage_size': stats.get('storageSize', 0),
                    'index_count': stats.get('nindexes', 0),
                    'index_size': stats.get('totalIndexSize', 0),
                    'average_object_size': stats.get('avgObjSize', 0),
                    'capped': stats.get('capped', False)
                }
            )
            
        except PyMongoError as e:
            logger.error(f"MongoDB metadata error: {e}")
            raise
    
    async def update_documents(self, query: dict, update: dict, upsert: bool = False) -> int:
        """MongoDB 도큐먼트 업데이트"""
        try:
            collection = await self._get_collection()
            
            result = await collection.update_many(query, update, upsert=upsert)
            
            logger.debug(f"Updated {result.modified_count} documents, upserted: {result.upserted_id}")
            return result.modified_count
            
        except PyMongoError as e:
            logger.error(f"MongoDB update error: {e}")
            raise
    
    async def aggregate(self, pipeline: List[dict]) -> List[dict]:
        """MongoDB 집계 파이프라인 실행"""
        try:
            collection = await self._get_collection()
            
            results = []
            async for doc in collection.aggregate(pipeline):
                serialized_doc = self._serialize_for_json(doc)
                results.append(serialized_doc)
            
            logger.debug(f"Aggregation returned {len(results)} results")
            return results
            
        except PyMongoError as e:
            logger.error(f"MongoDB aggregation error: {e}")
            raise
    
    async def create_index(self, keys: dict, **kwargs) -> str:
        """MongoDB 인덱스 생성"""
        try:
            collection = await self._get_collection()
            
            index_name = await collection.create_index(list(keys.items()), **kwargs)
            
            logger.debug(f"Created MongoDB index: {index_name}")
            return index_name
            
        except PyMongoError as e:
            logger.error(f"MongoDB index creation error: {e}")
            raise
    
    async def drop_collection(self) -> bool:
        """MongoDB 컬렉션 삭제"""
        try:
            collection = await self._get_collection()
            
            await collection.drop()
            
            logger.info(f"Dropped MongoDB collection: {self.config.collection}")
            return True
            
        except PyMongoError as e:
            logger.error(f"MongoDB collection drop error: {e}")
            raise
    
    async def read_stream(self, path: str = "") -> AsyncIterator[dict]:
        """MongoDB 도큐먼트 스트림 읽기 (대용량 컬렉션용)"""
        try:
            collection = await self._get_collection()
            
            cursor = collection.find(self.query, self.projection)
            
            if self.sort:
                cursor = cursor.sort(self.sort)
            if self.skip:
                cursor = cursor.skip(self.skip)
            if self.limit:
                cursor = cursor.limit(self.limit)
            
            async for doc in cursor:
                yield self._serialize_for_json(doc)
                
        except PyMongoError as e:
            logger.error(f"MongoDB stream read error: {e}")
            raise
    
    async def bulk_write(self, operations: List[dict]) -> dict:
        """MongoDB 대량 쓰기 작업"""
        try:
            collection = await self._get_collection()
            
            from pymongo import InsertOne, UpdateOne, DeleteOne, ReplaceOne
            
            bulk_ops = []
            for op in operations:
                op_type = op.get('type')
                if op_type == 'insert':
                    bulk_ops.append(InsertOne(op['document']))
                elif op_type == 'update':
                    bulk_ops.append(UpdateOne(op['filter'], op['update'], upsert=op.get('upsert', False)))
                elif op_type == 'delete':
                    bulk_ops.append(DeleteOne(op['filter']))
                elif op_type == 'replace':
                    bulk_ops.append(ReplaceOne(op['filter'], op['replacement'], upsert=op.get('upsert', False)))
            
            if bulk_ops:
                result = await collection.bulk_write(bulk_ops)
                
                return {
                    'inserted_count': result.inserted_count,
                    'modified_count': result.modified_count,
                    'deleted_count': result.deleted_count,
                    'upserted_count': result.upserted_count
                }
            else:
                return {'message': 'No operations to execute'}
                
        except PyMongoError as e:
            logger.error(f"MongoDB bulk write error: {e}")
            raise
    
    async def __aenter__(self):
        """비동기 컨텍스트 매니저 진입"""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """비동기 컨텍스트 매니저 종료"""
        if self._client:
            self._client.close()

# 사용 예제
"""
# MongoDB 소스 생성 - 조회
mongo_source = SourceMongoDB(params={
    'storage_config': {
        'host': 'localhost',
        'port': 27017,
        'database': 'analytics',
        'collection': 'user_events',
        'username': 'mongo_user',
        'password': 'password',
        'min_pool_size': 5,
        'max_pool_size': 20
    },
    'operation': 'read',
    'query': {'event_type': 'login', 'timestamp': {'$gte': '2024-01-01'}},
    'projection': {'user_id': 1, 'timestamp': 1, 'event_type': 1},
    'sort': [('timestamp', -1)],
    'limit': 1000
})

# 데이터 노드와 함께 사용
mongo_node = CommonNode(
    element="user_login_events",
    process=mongo_source
)

# MongoDB 쓰기
mongo_write = SourceMongoDB(params={
    'storage_config': {
        'host': 'localhost',
        'port': 27017,
        'database': 'analytics',
        'collection': 'processed_events'
    },
    'operation': 'write'
})

# 이전 노드 결과를 MongoDB에 저장
save_node = CommonNode(
    mongo_node,
    element="save_to_mongo",
    process=mongo_write
)

# 집계 파이프라인 실행
async def run_aggregation():
    mongo_agg = SourceMongoDB(params={
        'storage_config': {
            'host': 'localhost',
            'port': 27017,
            'database': 'analytics',
            'collection': 'user_events'
        }
    })
    
    pipeline = [
        {'$match': {'event_type': 'purchase'}},
        {'$group': {'_id': '$user_id', 'total_amount': {'$sum': '$amount'}}},
        {'$sort': {'total_amount': -1}},
        {'$limit': 10}
    ]
    
    results = await mongo_agg.aggregate(pipeline)
    return results

# 대량 데이터 스트림 처리
async def process_large_collection():
    mongo_stream = SourceMongoDB(params={
        'storage_config': {
            'host': 'localhost',
            'port': 27017,
            'database': 'analytics',
            'collection': 'large_events'
        },
        'query': {'processed': False},
        'limit': 10000
    })
    
    async for document in mongo_stream.read_stream():
        # 도큐먼트별 처리
        process_document(document)
"""