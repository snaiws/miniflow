# data/source/storage/gcs.py
import asyncio
import json
import pickle
from typing import Any, List, Optional, AsyncIterator
import tempfile
import os
import logging

import pandas as pd
from google.cloud import storage
from google.cloud.exceptions import NotFound, GoogleCloudError
from google.auth.credentials import Credentials
import aiofiles

from .base import ObjectStorageSource, StorageMetadata

logger = logging.getLogger(__name__)

class SourceGCS(ObjectStorageSource):
    """Google Cloud Storage 오브젝트 스토리지 소스"""
    
    def __init__(self, params: dict):
        super().__init__(params)
        self._client = None
        self._bucket = None
        
        # GCS 전용 설정
        self.credentials_path = params.get('credentials_path')
        self.project_id = params.get('project_id')
    
    async def _get_client(self):
        """GCS 클라이언트 가져오기 (지연 초기화)"""
        if self._client is None:
            if self.credentials_path:
                # 서비스 계정 키 파일 사용
                self._client = await asyncio.to_thread(
                    storage.Client.from_service_account_json,
                    self.credentials_path,
                    project=self.project_id
                )
            else:
                # 기본 인증 사용 (환경 변수 등)
                self._client = await asyncio.to_thread(
                    storage.Client,
                    project=self.project_id
                )
            
            logger.debug(f"Connected to GCS project: {self.project_id}")
        
        return self._client
    
    async def _get_bucket(self):
        """GCS 버킷 가져오기"""
        if self._bucket is None:
            client = await self._get_client()
            self._bucket = await asyncio.to_thread(
                client.bucket,
                self.config.bucket_name
            )
        return self._bucket
    
    def _get_blob_name(self, path: str) -> str:
        """GCS 블롭 이름 생성"""
        return path.lstrip('/')  # GCS 블롭 이름은 /로 시작하면 안됨
    
    async def read(self, path: str) -> Any:
        """GCS 블롭 읽기"""
        try:
            bucket = await self._get_bucket()
            blob_name = self._get_blob_name(path)
            
            blob = await asyncio.to_thread(bucket.blob, blob_name)
            
            logger.debug(f"Reading GCS blob: gs://{self.config.bucket_name}/{blob_name}")
            
            # 블롭 존재 확인
            exists = await asyncio.to_thread(blob.exists)
            if not exists:
                raise FileNotFoundError(f"GCS blob not found: gs://{self.config.bucket_name}/{blob_name}")
            
            # 블롭 다운로드
            content = await asyncio.to_thread(blob.download_as_bytes)
            
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
                
        except GoogleCloudError as e:
            logger.error(f"GCS read error: {e}")
            raise
        except Exception as e:
            logger.error(f"GCS read error for {path}: {e}")
            raise
    
    async def write(self, path: str, data: Any) -> bool:
        """GCS 블롭 쓰기"""
        try:
            bucket = await self._get_bucket()
            blob_name = self._get_blob_name(path)
            
            blob = await asyncio.to_thread(bucket.blob, blob_name)
            
            # 데이터 타입에 따라 변환
            content_type = 'application/octet-stream'
            
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
            else:
                # pickle로 직렬화
                content = pickle.dumps(data)
            
            logger.debug(f"Writing GCS blob: gs://{self.config.bucket_name}/{blob_name} ({len(content)} bytes)")
            
            # 메타데이터 설정
            blob.content_type = content_type
            
            # 멀티파트 업로드가 필요한 경우
            if len(content) > self.config.multipart_threshold:
                await self._multipart_upload(blob, content)
            else:
                await asyncio.to_thread(blob.upload_from_string, content)
            
            return True
            
        except GoogleCloudError as e:
            logger.error(f"GCS write error: {e}")
            raise
        except Exception as e:
            logger.error(f"GCS write error for {path}: {e}")
            raise
    
    async def _multipart_upload(self, blob, content: bytes):
        """GCS 멀티파트 업로드"""
        try:
            # 임시 파일을 사용한 청크 업로드
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                tmp_file.write(content)
                tmp_file.flush()
                
                # 청크 크기로 업로드
                await asyncio.to_thread(
                    blob.upload_from_filename,
                    tmp_file.name,
                    chunk_size=self.config.multipart_chunksize
                )
                
            # 임시 파일 삭제
            os.unlink(tmp_file.name)
            
            logger.debug(f"Multipart upload completed: {blob.name}")
            
        except Exception as e:
            logger.error(f"GCS multipart upload failed: {e}")
            raise
    
    async def list_objects(self, path: str = "", pattern: str = "*", recursive: bool = False) -> List[str]:
        """GCS 블롭 목록 조회"""
        try:
            bucket = await self._get_bucket()
            prefix = self._get_blob_name(path)
            
            if not recursive and prefix and not prefix.endswith('/'):
                prefix += '/'
            
            # 구분자 설정
            delimiter = None if recursive else '/'
            
            blobs = await asyncio.to_thread(
                list,
                bucket.list_blobs(prefix=prefix, delimiter=delimiter)
            )
            
            blob_names = []
            for blob in blobs:
                # 패턴 매칭
                if pattern == "*" or self._match_pattern(blob.name, pattern):
                    blob_names.append(blob.name)
            
            return blob_names
            
        except GoogleCloudError as e:
            logger.error(f"GCS list error: {e}")
            raise
    
    def _match_pattern(self, blob_name: str, pattern: str) -> bool:
        """간단한 패턴 매칭"""
        import fnmatch
        return fnmatch.fnmatch(blob_name, pattern)
    
    async def delete(self, path: str) -> bool:
        """GCS 블롭 삭제"""
        try:
            bucket = await self._get_bucket()
            blob_name = self._get_blob_name(path)
            
            blob = await asyncio.to_thread(bucket.blob, blob_name)
            
            # 존재 확인 후 삭제
            exists = await asyncio.to_thread(blob.exists)
            if exists:
                await asyncio.to_thread(blob.delete)
                logger.debug(f"Deleted GCS blob: gs://{self.config.bucket_name}/{blob_name}")
                return True
            else:
                logger.warning(f"GCS blob not found for deletion: {blob_name}")
                return False
                
        except GoogleCloudError as e:
            logger.error(f"GCS delete error: {e}")
            raise
    
    async def exists(self, path: str) -> bool:
        """GCS 블롭 존재 확인"""
        try:
            bucket = await self._get_bucket()
            blob_name = self._get_blob_name(path)
            
            blob = await asyncio.to_thread(bucket.blob, blob_name)
            return await asyncio.to_thread(blob.exists)
            
        except GoogleCloudError as e:
            logger.error(f"GCS exists check error: {e}")
            raise
    
    async def get_metadata(self, path: str) -> StorageMetadata:
        """GCS 블롭 메타데이터 조회"""
        try:
            bucket = await self._get_bucket()
            blob_name = self._get_blob_name(path)
            
            blob = await asyncio.to_thread(bucket.blob, blob_name)
            
            # 메타데이터 새로고침
            await asyncio.to_thread(blob.reload)
            
            return StorageMetadata(
                size=blob.size,
                created_time=blob.time_created.isoformat() if blob.time_created else None,
                modified_time=blob.updated.isoformat() if blob.updated else None,
                content_type=blob.content_type,
                etag=blob.etag,
                custom_metadata=blob.metadata or {}
            )
            
        except NotFound:
            raise FileNotFoundError(f"GCS blob not found: gs://{self.config.bucket_name}/{blob_name}")
        except GoogleCloudError as e:
            logger.error(f"GCS metadata error: {e}")
            raise
    
    async def create_bucket(self, bucket_name: str, location: str = "US") -> bool:
        """GCS 버킷 생성"""
        try:
            client = await self._get_client()
            
            bucket = await asyncio.to_thread(client.bucket, bucket_name)
            bucket.storage_class = "STANDARD"
            
            await asyncio.to_thread(
                client.create_bucket,
                bucket,
                location=location
            )
            
            logger.info(f"Created GCS bucket: {bucket_name} in {location}")
            return True
            
        except GoogleCloudError as e:
            if "already exists" in str(e).lower():
                logger.warning(f"GCS bucket already exists: {bucket_name}")
                return True
            else:
                logger.error(f"GCS bucket creation error: {e}")
                raise
    
    async def delete_bucket(self, bucket_name: str) -> bool:
        """GCS 버킷 삭제"""
        try:
            client = await self._get_client()
            bucket = await asyncio.to_thread(client.bucket, bucket_name)
            
            await asyncio.to_thread(bucket.delete)
            
            logger.info(f"Deleted GCS bucket: {bucket_name}")
            return True
            
        except NotFound:
            logger.warning(f"GCS bucket not found for deletion: {bucket_name}")
            return False
        except GoogleCloudError as e:
            logger.error(f"GCS bucket deletion error: {e}")
            raise
    
    async def list_buckets(self) -> List[str]:
        """GCS 버킷 목록 조회"""
        try:
            client = await self._get_client()
            
            buckets = await asyncio.to_thread(list, client.list_buckets())
            return [bucket.name for bucket in buckets]
            
        except GoogleCloudError as e:
            logger.error(f"GCS list buckets error: {e}")
            raise
    
    async def copy(self, source_path: str, dest_path: str) -> bool:
        """GCS 블롭 복사 (서버 사이드 복사)"""
        try:
            bucket = await self._get_bucket()
            source_blob_name = self._get_blob_name(source_path)
            dest_blob_name = self._get_blob_name(dest_path)
            
            source_blob = await asyncio.to_thread(bucket.blob, source_blob_name)
            dest_blob = await asyncio.to_thread(bucket.blob, dest_blob_name)
            
            await asyncio.to_thread(bucket.copy_blob, source_blob, bucket, dest_blob_name)
            
            logger.debug(f"Copied GCS blob: {source_blob_name} -> {dest_blob_name}")
            return True
            
        except GoogleCloudError as e:
            logger.error(f"GCS copy error: {e}")
            raise
    
    async def read_stream(self, path: str) -> AsyncIterator[bytes]:
        """GCS 블롭 스트림 읽기"""
        try:
            bucket = await self._get_bucket()
            blob_name = self._get_blob_name(path)
            
            blob = await asyncio.to_thread(bucket.blob, blob_name)
            
            # 청크별로 다운로드
            with tempfile.NamedTemporaryFile() as tmp_file:
                await asyncio.to_thread(
                    blob.download_to_filename,
                    tmp_file.name,
                    chunk_size=self.config.chunk_size
                )
                
                # 파일에서 청크별로 읽기
                async with aiofiles.open(tmp_file.name, 'rb') as f:
                    while True:
                        chunk = await f.read(self.config.chunk_size)
                        if not chunk:
                            break
                        yield chunk
                        
        except GoogleCloudError as e:
            logger.error(f"GCS stream read error for {path}: {e}")
            raise
    
    async def set_blob_metadata(self, path: str, metadata: dict) -> bool:
        """GCS 블롭 메타데이터 설정"""
        try:
            bucket = await self._get_bucket()
            blob_name = self._get_blob_name(path)
            
            blob = await asyncio.to_thread(bucket.blob, blob_name)
            blob.metadata = metadata
            
            await asyncio.to_thread(blob.patch)
            
            logger.debug(f"Set GCS blob metadata: {blob_name}")
            return True
            
        except GoogleCloudError as e:
            logger.error(f"GCS set metadata error: {e}")
            raise
    
    async def make_public(self, path: str) -> str:
        """GCS 블롭을 공개 읽기로 설정하고 공개 URL 반환"""
        try:
            bucket = await self._get_bucket()
            blob_name = self._get_blob_name(path)
            
            blob = await asyncio.to_thread(bucket.blob, blob_name)
            await asyncio.to_thread(blob.make_public)
            
            public_url = blob.public_url
            logger.debug(f"Made GCS blob public: {blob_name} -> {public_url}")
            
            return public_url
            
        except GoogleCloudError as e:
            logger.error(f"GCS make public error: {e}")
            raise
    
    async def generate_signed_url(self, path: str, expiration_minutes: int = 60) -> str:
        """GCS 블롭의 서명된 URL 생성"""
        try:
            from datetime import timedelta
            
            bucket = await self._get_bucket()
            blob_name = self._get_blob_name(path)
            
            blob = await asyncio.to_thread(bucket.blob, blob_name)
            
            signed_url = await asyncio.to_thread(
                blob.generate_signed_url,
                expiration=timedelta(minutes=expiration_minutes),
                method="GET"
            )
            
            logger.debug(f"Generated signed URL for GCS blob: {blob_name}")
            return signed_url
            
        except GoogleCloudError as e:
            logger.error(f"GCS signed URL generation error: {e}")
            raise

# 사용 예제
"""
# GCS 소스 생성
gcs_source = SourceGCS(params={
    'storage_config': {
        'bucket_name': 'my-data-bucket',
        'multipart_threshold': 64 * 1024 * 1024,  # 64MB
        'multipart_chunksize': 16 * 1024 * 1024,  # 16MB
        'retry_attempts': 3
    },
    'credentials_path': '/path/to/service-account.json',
    'project_id': 'my-gcp-project',
    'operation': 'read',
    'path': 'data/analytics/user_events.csv'
})

# 데이터 노드와 함께 사용
gcs_node = CommonNode(
    element="gcs_data",
    process=gcs_source
)

result = await gcs_node.get_data()

# GCS 쓰기
gcs_write = SourceGCS(params={
    'storage_config': {
        'bucket_name': 'my-processed-data',
    },
    'credentials_path': '/path/to/service-account.json',
    'project_id': 'my-gcp-project',
    'operation': 'write',
    'path': 'processed/results.parquet'
})

# 이전 노드 결과를 GCS에 저장
save_node = CommonNode(
    gcs_node,
    element="save_to_gcs",
    process=gcs_write
)

# 대용량 파일 스트림 처리
async def process_large_gcs_file():
    gcs_stream = SourceGCS(params={
        'storage_config': {
            'bucket_name': 'my-large-files',
            'chunk_size': 1024 * 1024  # 1MB 청크
        },
        'credentials_path': '/path/to/service-account.json',
        'project_id': 'my-gcp-project'
    })
    
    async for chunk in gcs_stream.read_stream('large_data/big_file.bin'):
        # 청크별 처리
        process_chunk(chunk)

# 공개 URL 생성
async def make_file_public():
    gcs_source = SourceGCS(params={
        'storage_config': {'bucket_name': 'my-public-bucket'},
        'credentials_path': '/path/to/service-account.json',
        'project_id': 'my-gcp-project'
    })
    
    public_url = await gcs_source.make_public('reports/monthly_report.pdf')
    print(f"Public URL: {public_url}")

# 서명된 URL 생성 (임시 접근)
async def create_temp_access():
    gcs_source = SourceGCS(params={
        'storage_config': {'bucket_name': 'my-private-bucket'},
        'credentials_path': '/path/to/service-account.json',
        'project_id': 'my-gcp-project'
    })
    
    signed_url = await gcs_source.generate_signed_url(
        'private/sensitive_data.csv',
        expiration_minutes=30
    )
    print(f"Temporary access URL (30 min): {signed_url}")
"""