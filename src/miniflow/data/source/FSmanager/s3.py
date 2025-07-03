# data/source/storage/s3.py
import asyncio
import json
import pickle
from typing import Any, List, Optional, AsyncIterator
from dataclasses import dataclass
import logging

import aioboto3
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError

from .base import ObjectStorageSource, StorageMetadata

logger = logging.getLogger(__name__)

class SourceS3(ObjectStorageSource):
    """AWS S3 오브젝트 스토리지 소스"""
    
    def __init__(self, params: dict):
        super().__init__(params)
        self._session = None
        self._client = None
    
    async def _get_client(self):
        """S3 클라이언트 가져오기 (지연 초기화)"""
        if self._client is None:
            if self._session is None:
                self._session = aioboto3.Session(
                    aws_access_key_id=self.config.access_key,
                    aws_secret_access_key=self.config.secret_key,
                    region_name=self.config.region
                )
            
            self._client = self._session.client(
                's3',
                endpoint_url=self.config.endpoint_url,
                config=aioboto3.session.Config(
                    connect_timeout=self.config.connection_timeout,
                    read_timeout=self.config.read_timeout,
                    retries={'max_attempts': self.config.retry_attempts}
                )
            )
        
        return self._client
    
    def _get_full_key(self, path: str) -> str:
        """전체 S3 키 생성"""
        return path.lstrip('/')  # S3 키는 /로 시작하면 안됨
    
    async def read(self, path: str) -> Any:
        """S3 객체 읽기"""
        try:
            client = await self._get_client()
            key = self._get_full_key(path)
            
            logger.debug(f"Reading S3 object: s3://{self.config.bucket_name}/{key}")
            
            async with client as s3:
                response = await s3.get_object(Bucket=self.config.bucket_name, Key=key)
                content = await response['Body'].read()
            
            # 파일 확장자에 따라 자동 변환
            if path.endswith('.json'):
                return json.loads(content.decode('utf-8'))
            elif path.endswith('.csv'):
                import io
                return pd.read_csv(io.BytesIO(content))
            elif path.endswith('.parquet'):
                import io
                return pd.read_parquet(io.BytesIO(content))
            elif path.endswith('.pkl') or path.endswith('.pickle'):
                return pickle.loads(content)
            elif path.endswith(('.txt', '.log')):
                return content.decode('utf-8')
            else:
                return content  # 바이너리 그대로 반환
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                raise FileNotFoundError(f"S3 object not found: s3://{self.config.bucket_name}/{key}")
            elif error_code == 'NoSuchBucket':
                raise FileNotFoundError(f"S3 bucket not found: {self.config.bucket_name}")
            else:
                logger.error(f"S3 read error: {e}")
                raise
    
    async def write(self, path: str, data: Any) -> bool:
        """S3 객체 쓰기"""
        try:
            client = await self._get_client()
            key = self._get_full_key(path)
            
            # 데이터 타입에 따라 변환
            if isinstance(data, dict) or isinstance(data, list):
                content = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')
                content_type = 'application/json'
            elif isinstance(data, pd.DataFrame):
                if path.endswith('.csv'):
                    content = data.to_csv(index=False).encode('utf-8')
                    content_type = 'text/csv'
                elif path.endswith('.parquet'):
                    import io
                    buffer = io.BytesIO()
                    data.to_parquet(buffer)
                    content = buffer.getvalue()
                    content_type = 'application/octet-stream'
                else:
                    content = data.to_json(orient='records').encode('utf-8')
                    content_type = 'application/json'
            elif isinstance(data, str):
                content = data.encode('utf-8')
                content_type = 'text/plain'
            elif isinstance(data, bytes):
                content = data
                content_type = 'application/octet-stream'
            else:
                # pickle로 직렬화
                content = pickle.dumps(data)
                content_type = 'application/octet-stream'
            
            logger.debug(f"Writing S3 object: s3://{self.config.bucket_name}/{key} ({len(content)} bytes)")
            
            async with client as s3:
                # 멀티파트 업로드가 필요한 경우
                if len(content) > self.config.multipart_threshold:
                    await self._multipart_upload(s3, key, content, content_type)
                else:
                    await s3.put_object(
                        Bucket=self.config.bucket_name,
                        Key=key,
                        Body=content,
                        ContentType=content_type
                    )
            
            return True
            
        except Exception as e:
            logger.error(f"S3 write error for {path}: {e}")
            raise
    
    async def _multipart_upload(self, s3_client, key: str, content: bytes, content_type: str):
        """멀티파트 업로드"""
        response = await s3_client.create_multipart_upload(
            Bucket=self.config.bucket_name,
            Key=key,
            ContentType=content_type
        )
        
        upload_id = response['UploadId']
        parts = []
        
        try:
            # 청크별로 업로드
            chunk_size = self.config.multipart_chunksize
            part_number = 1
            
            for i in range(0, len(content), chunk_size):
                chunk = content[i:i + chunk_size]
                
                response = await s3_client.upload_part(
                    Bucket=self.config.bucket_name,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=chunk
                )
                
                parts.append({
                    'ETag': response['ETag'],
                    'PartNumber': part_number
                })
                
                part_number += 1
            
            # 업로드 완료
            await s3_client.complete_multipart_upload(
                Bucket=self.config.bucket_name,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            
            logger.debug(f"Multipart upload completed: {key} ({len(parts)} parts)")
            
        except Exception as e:
            # 실패시 업로드 중단
            await s3_client.abort_multipart_upload(
                Bucket=self.config.bucket_name,
                Key=key,
                UploadId=upload_id
            )
            logger.error(f"Multipart upload failed for {key}: {e}")
            raise
    
    async def list_objects(self, path: str = "", pattern: str = "*", recursive: bool = False) -> List[str]:
        """S3 객체 목록 조회"""
        try:
            client = await self._get_client()
            prefix = self._get_full_key(path)
            
            if not recursive and not prefix.endswith('/') and prefix:
                prefix += '/'
            
            objects = []
            async with client as s3:
                paginator = s3.get_paginator('list_objects_v2')
                
                params = {
                    'Bucket': self.config.bucket_name,
                    'Prefix': prefix
                }
                
                if not recursive:
                    params['Delimiter'] = '/'
                
                async for page in paginator.paginate(**params):
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            key = obj['Key']
                            
                            # 패턴 매칭 (간단한 와일드카드)
                            if pattern == "*" or self._match_pattern(key, pattern):
                                objects.append(key)
            
            return objects
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                raise FileNotFoundError(f"S3 bucket not found: {self.config.bucket_name}")
            else:
                logger.error(f"S3 list error: {e}")
                raise
    
    def _match_pattern(self, key: str, pattern: str) -> bool:
        """간단한 패턴 매칭"""
        import fnmatch
        return fnmatch.fnmatch(key, pattern)
    
    async def delete(self, path: str) -> bool:
        """S3 객체 삭제"""
        try:
            client = await self._get_client()
            key = self._get_full_key(path)
            
            async with client as s3:
                await s3.delete_object(Bucket=self.config.bucket_name, Key=key)
            
            logger.debug(f"Deleted S3 object: s3://{self.config.bucket_name}/{key}")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                logger.warning(f"S3 object not found for deletion: {key}")
                return False
            else:
                logger.error(f"S3 delete error: {e}")
                raise
    
    async def exists(self, path: str) -> bool:
        """S3 객체 존재 확인"""
        try:
            client = await self._get_client()
            key = self._get_full_key(path)
            
            async with client as s3:
                await s3.head_object(Bucket=self.config.bucket_name, Key=key)
            
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['NoSuchKey', '404']:
                return False
            else:
                logger.error(f"S3 exists check error: {e}")
                raise
    
    async def get_metadata(self, path: str) -> StorageMetadata:
        """S3 객체 메타데이터 조회"""
        try:
            client = await self._get_client()
            key = self._get_full_key(path)
            
            async with client as s3:
                response = await s3.head_object(Bucket=self.config.bucket_name, Key=key)
            
            return StorageMetadata(
                size=response.get('ContentLength'),
                created_time=response.get('LastModified'),
                modified_time=response.get('LastModified'),
                content_type=response.get('ContentType'),
                etag=response.get('ETag'),
                custom_metadata=response.get('Metadata', {})
            )
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['NoSuchKey', '404']:
                raise FileNotFoundError(f"S3 object not found: s3://{self.config.bucket_name}/{key}")
            else:
                logger.error(f"S3 metadata error: {e}")
                raise
    
    async def create_bucket(self, bucket_name: str) -> bool:
        """S3 버킷 생성"""
        try:
            client = await self._get_client()
            
            async with client as s3:
                if self.config.region != 'us-east-1':
                    await s3.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': self.config.region}
                    )
                else:
                    await s3.create_bucket(Bucket=bucket_name)
            
            logger.info(f"Created S3 bucket: {bucket_name}")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'BucketAlreadyExists':
                logger.warning(f"S3 bucket already exists: {bucket_name}")
                return True
            else:
                logger.error(f"S3 bucket creation error: {e}")
                raise
    
    async def delete_bucket(self, bucket_name: str) -> bool:
        """S3 버킷 삭제"""
        try:
            client = await self._get_client()
            
            async with client as s3:
                await s3.delete_bucket(Bucket=bucket_name)
            
            logger.info(f"Deleted S3 bucket: {bucket_name}")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                logger.warning(f"S3 bucket not found for deletion: {bucket_name}")
                return False
            else:
                logger.error(f"S3 bucket deletion error: {e}")
                raise
    
    async def list_buckets(self) -> List[str]:
        """S3 버킷 목록 조회"""
        try:
            client = await self._get_client()
            
            async with client as s3:
                response = await s3.list_buckets()
            
            return [bucket['Name'] for bucket in response['Buckets']]
            
        except Exception as e:
            logger.error(f"S3 list buckets error: {e}")
            raise
    
    async def read_stream(self, path: str) -> AsyncIterator[bytes]:
        """S3 객체 스트림 읽기"""
        try:
            client = await self._get_client()
            key = self._get_full_key(path)
            
            async with client as s3:
                response = await s3.get_object(Bucket=self.config.bucket_name, Key=key)
                
                async for chunk in response['Body'].iter_chunks(chunk_size=self.config.chunk_size):
                    yield chunk
                    
        except Exception as e:
            logger.error(f"S3 stream read error for {path}: {e}")
            raise
    
    async def copy(self, source_path: str, dest_path: str) -> bool:
        """S3 객체 복사 (서버 사이드 복사)"""
        try:
            client = await self._get_client()
            source_key = self._get_full_key(source_path)
            dest_key = self._get_full_key(dest_path)
            
            copy_source = {
                'Bucket': self.config.bucket_name,
                'Key': source_key
            }
            
            async with client as s3:
                await s3.copy_object(
                    CopySource=copy_source,
                    Bucket=self.config.bucket_name,
                    Key=dest_key
                )
            
            logger.debug(f"Copied S3 object: {source_key} -> {dest_key}")
            return True
            
        except Exception as e:
            logger.error(f"S3 copy error: {e}")
            raise
    
    async def __aenter__(self):
        """비동기 컨텍스트 매니저 진입"""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """비동기 컨텍스트 매니저 종료"""
        if self._client:
            await self._client.close()

# 사용 예제
"""
# S3 소스 생성
s3_source = SourceS3(params={
    'storage_config': {
        'bucket_name': 'my-data-bucket',
        'access_key': 'AKIAIOSFODNN7EXAMPLE',
        'secret_key': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        'region': 'us-west-2',
        'multipart_threshold': 64 * 1024 * 1024,  # 64MB
        'retry_attempts': 3
    },
    'operation': 'read',
    'path': 'data/users.csv'
})

# 데이터 노드와 함께 사용
s3_node = CommonNode(
    element="s3_data",
    process=s3_source
)

result = await s3_node.get_data()

# 쓰기 예제
s3_write = SourceS3(params={
    'storage_config': {
        'bucket_name': 'my-data-bucket',
        'access_key': 'xxx',
        'secret_key': 'yyy'
    },
    'operation': 'write',
    'path': 'processed/results.json'
})

# 이전 노드 결과를 S3에 저장
save_node = CommonNode(
    s3_node,
    element="save_to_s3",
    process=s3_write
)
"""