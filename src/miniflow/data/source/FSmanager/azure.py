# data/source/storage/azure_blob.py
import asyncio
import json
import pickle
from typing import Any, List, Optional, AsyncIterator
import tempfile
import os
import logging

import pandas as pd
from azure.storage.blob.aio import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError, AzureError
from azure.storage.blob import BlobSasPermissions, generate_blob_sas
from datetime import datetime, timedelta

from .base import ObjectStorageSource, StorageMetadata

logger = logging.getLogger(__name__)

class SourceAzureBlob(ObjectStorageSource):
    """Azure Blob Storage 오브젝트 스토리지 소스"""
    
    def __init__(self, params: dict):
        super().__init__(params)
        self._service_client = None
        self._container_client = None
        
        # Azure 전용 설정
        self.account_name = params.get('account_name', '')
        self.account_key = params.get('account_key', '')
        self.connection_string = params.get('connection_string', '')
        self.container_name = self.config.bucket_name  # bucket_name을 container_name으로 사용
    
    async def _get_service_client(self):
        """Azure Blob Service 클라이언트 가져오기 (지연 초기화)"""
        if self._service_client is None:
            if self.connection_string:
                self._service_client = BlobServiceClient.from_connection_string(
                    self.connection_string
                )
            elif self.account_name and self.account_key:
                account_url = f"https://{self.account_name}.blob.core.windows.net"
                self._service_client = BlobServiceClient(
                    account_url=account_url,
                    credential=self.account_key
                )
            else:
                raise ValueError("Either connection_string or account_name+account_key must be provided")
            
            logger.debug(f"Connected to Azure Blob Storage: {self.account_name}")
        
        return self._service_client
    
    async def _get_container_client(self):
        """Azure Container 클라이언트 가져오기"""
        if self._container_client is None:
            service_client = await self._get_service_client()
            self._container_client = service_client.get_container_client(self.container_name)
        return self._container_client
    
    def _get_blob_name(self, path: str) -> str:
        """Azure 블롭 이름 생성"""
        return path.lstrip('/')
    
    async def read(self, path: str) -> Any:
        """Azure 블롭 읽기"""
        try:
            container_client = await self._get_container_client()
            blob_name = self._get_blob_name(path)
            
            logger.debug(f"Reading Azure blob: {self.container_name}/{blob_name}")
            
            # 블롭 다운로드
            blob_client = container_client.get_blob_client(blob_name)
            
            # 스트림으로 다운로드
            stream = await blob_client.download_blob()
            content = await stream.readall()
            
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
                
        except ResourceNotFoundError:
            raise FileNotFoundError(f"Azure blob not found: {self.container_name}/{blob_name}")
        except AzureError as e:
            logger.error(f"Azure read error: {e}")
            raise
        except Exception as e:
            logger.error(f"Azure read error for {path}: {e}")
            raise
    
    async def write(self, path: str, data: Any) -> bool:
        """Azure 블롭 쓰기"""
        try:
            container_client = await self._get_container_client()
            blob_name = self._get_blob_name(path)
            
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
            
            logger.debug(f"Writing Azure blob: {self.container_name}/{blob_name} ({len(content)} bytes)")
            
            # 블롭 클라이언트 가져오기
            blob_client = container_client.get_blob_client(blob_name)
            
            # 메타데이터 설정
            blob_properties = {
                'content_type': content_type,
                'content_length': len(content)
            }
            
            # 멀티파트 업로드가 필요한 경우
            if len(content) > self.config.multipart_threshold:
                await self._multipart_upload(blob_client, content, **blob_properties)
            else:
                await blob_client.upload_blob(
                    content,
                    overwrite=True,
                    content_settings=blob_properties
                )
            
            return True
            
        except AzureError as e:
            logger.error(f"Azure write error: {e}")
            raise
        except Exception as e:
            logger.error(f"Azure write error for {path}: {e}")
            raise
    
    async def _multipart_upload(self, blob_client, content: bytes, **properties):
        """Azure 청크 업로드"""
        try:
            # 청크 크기로 업로드
            await blob_client.upload_blob(
                content,
                overwrite=True,
                max_single_put_size=self.config.multipart_chunksize,
                max_block_size=self.config.multipart_chunksize,
                content_settings=properties
            )
            
            logger.debug(f"Multipart upload completed: {blob_client.blob_name}")
            
        except Exception as e:
            logger.error(f"Azure multipart upload failed: {e}")
            raise
    
    async def list_objects(self, path: str = "", pattern: str = "*", recursive: bool = False) -> List[str]:
        """Azure 블롭 목록 조회"""
        try:
            container_client = await self._get_container_client()
            prefix = self._get_blob_name(path)
            
            if not recursive and prefix and not prefix.endswith('/'):
                prefix += '/'
            
            blob_names = []
            
            # 블롭 목록 조회
            async for blob in container_client.list_blobs(name_starts_with=prefix):
                blob_name = blob.name
                
                # 재귀가 아닌 경우 직접 하위만
                if not recursive and prefix:
                    relative_path = blob_name[len(prefix):]
                    if '/' in relative_path:
                        continue  # 하위 디렉토리 내 파일 제외
                
                # 패턴 매칭
                if pattern == "*" or self._match_pattern(blob_name, pattern):
                    blob_names.append(blob_name)
            
            return blob_names
            
        except AzureError as e:
            logger.error(f"Azure list error: {e}")
            raise
    
    def _match_pattern(self, blob_name: str, pattern: str) -> bool:
        """간단한 패턴 매칭"""
        import fnmatch
        return fnmatch.fnmatch(blob_name, pattern)
    
    async def delete(self, path: str) -> bool:
        """Azure 블롭 삭제"""
        try:
            container_client = await self._get_container_client()
            blob_name = self._get_blob_name(path)
            
            blob_client = container_client.get_blob_client(blob_name)
            
            try:
                await blob_client.delete_blob()
                logger.debug(f"Deleted Azure blob: {self.container_name}/{blob_name}")
                return True
            except ResourceNotFoundError:
                logger.warning(f"Azure blob not found for deletion: {blob_name}")
                return False
                
        except AzureError as e:
            logger.error(f"Azure delete error: {e}")
            raise
    
    async def exists(self, path: str) -> bool:
        """Azure 블롭 존재 확인"""
        try:
            container_client = await self._get_container_client()
            blob_name = self._get_blob_name(path)
            
            blob_client = container_client.get_blob_client(blob_name)
            
            try:
                await blob_client.get_blob_properties()
                return True
            except ResourceNotFoundError:
                return False
                
        except AzureError as e:
            logger.error(f"Azure exists check error: {e}")
            raise
    
    async def get_metadata(self, path: str) -> StorageMetadata:
        """Azure 블롭 메타데이터 조회"""
        try:
            container_client = await self._get_container_client()
            blob_name = self._get_blob_name(path)
            
            blob_client = container_client.get_blob_client(blob_name)
            
            try:
                properties = await blob_client.get_blob_properties()
                
                return StorageMetadata(
                    size=properties.size,
                    created_time=properties.creation_time.isoformat() if properties.creation_time else None,
                    modified_time=properties.last_modified.isoformat() if properties.last_modified else None,
                    content_type=properties.content_settings.content_type if properties.content_settings else None,
                    etag=properties.etag,
                    custom_metadata=properties.metadata or {}
                )
            except ResourceNotFoundError:
                raise FileNotFoundError(f"Azure blob not found: {self.container_name}/{blob_name}")
                
        except AzureError as e:
            logger.error(f"Azure metadata error: {e}")
            raise
    
    async def create_bucket(self, bucket_name: str) -> bool:
        """Azure 컨테이너 생성"""
        try:
            service_client = await self._get_service_client()
            
            container_client = service_client.get_container_client(bucket_name)
            await container_client.create_container()
            
            logger.info(f"Created Azure container: {bucket_name}")
            return True
            
        except AzureError as e:
            if "already exists" in str(e).lower():
                logger.warning(f"Azure container already exists: {bucket_name}")
                return True
            else:
                logger.error(f"Azure container creation error: {e}")
                raise
    
    async def delete_bucket(self, bucket_name: str) -> bool:
        """Azure 컨테이너 삭제"""
        try:
            service_client = await self._get_service_client()
            
            container_client = service_client.get_container_client(bucket_name)
            await container_client.delete_container()
            
            logger.info(f"Deleted Azure container: {bucket_name}")
            return True
            
        except ResourceNotFoundError:
            logger.warning(f"Azure container not found for deletion: {bucket_name}")
            return False
        except AzureError as e:
            logger.error(f"Azure container deletion error: {e}")
            raise
    
    async def list_buckets(self) -> List[str]:
        """Azure 컨테이너 목록 조회"""
        try:
            service_client = await self._get_service_client()
            
            containers = []
            async for container in service_client.list_containers():
                containers.append(container.name)
            
            return containers
            
        except AzureError as e:
            logger.error(f"Azure list containers error: {e}")
            raise
    
    async def copy(self, source_path: str, dest_path: str) -> bool:
        """Azure 블롭 복사 (서버 사이드 복사)"""
        try:
            container_client = await self._get_container_client()
            source_blob_name = self._get_blob_name(source_path)
            dest_blob_name = self._get_blob_name(dest_path)
            
            source_blob_client = container_client.get_blob_client(source_blob_name)
            dest_blob_client = container_client.get_blob_client(dest_blob_name)
            
            # 소스 블롭 URL 생성
            source_url = source_blob_client.url
            
            # 복사 시작
            copy_props = await dest_blob_client.start_copy_from_url(source_url)
            
            # 복사 완료 대기
            copy_id = copy_props['copy_id']
            while True:
                props = await dest_blob_client.get_blob_properties()
                if props.copy.status == 'success':
                    break
                elif props.copy.status == 'failed':
                    raise Exception(f"Copy failed: {props.copy.status_description}")
                await asyncio.sleep(1)
            
            logger.debug(f"Copied Azure blob: {source_blob_name} -> {dest_blob_name}")
            return True
            
        except AzureError as e:
            logger.error(f"Azure copy error: {e}")
            raise
    
    async def read_stream(self, path: str) -> AsyncIterator[bytes]:
        """Azure 블롭 스트림 읽기"""
        try:
            container_client = await self._get_container_client()
            blob_name = self._get_blob_name(path)
            
            blob_client = container_client.get_blob_client(blob_name)
            
            # 스트림으로 다운로드
            stream = await blob_client.download_blob()
            
            async for chunk in stream.chunks():
                yield chunk
                        
        except AzureError as e:
            logger.error(f"Azure stream read error for {path}: {e}")
            raise
    
    async def set_blob_metadata(self, path: str, metadata: dict) -> bool:
        """Azure 블롭 메타데이터 설정"""
        try:
            container_client = await self._get_container_client()
            blob_name = self._get_blob_name(path)
            
            blob_client = container_client.get_blob_client(blob_name)
            await blob_client.set_blob_metadata(metadata)
            
            logger.debug(f"Set Azure blob metadata: {blob_name}")
            return True
            
        except AzureError as e:
            logger.error(f"Azure set metadata error: {e}")
            raise
    
    async def set_blob_tier(self, path: str, tier: str) -> bool:
        """Azure 블롭 액세스 계층 설정 (Hot, Cool, Archive)"""
        try:
            container_client = await self._get_container_client()
            blob_name = self._get_blob_name(path)
            
            blob_client = container_client.get_blob_client(blob_name)
            await blob_client.set_standard_blob_tier(tier)
            
            logger.debug(f"Set Azure blob tier: {blob_name} -> {tier}")
            return True
            
        except AzureError as e:
            logger.error(f"Azure set tier error: {e}")
            raise
    
    async def generate_sas_url(self, path: str, expiration_hours: int = 1, permissions: str = "r") -> str:
        """Azure 블롭의 SAS URL 생성"""
        try:
            blob_name = self._get_blob_name(path)
            
            # SAS 권한 설정
            permission_map = {
                'r': BlobSasPermissions(read=True),
                'w': BlobSasPermissions(write=True),
                'rw': BlobSasPermissions(read=True, write=True),
                'rwl': BlobSasPermissions(read=True, write=True, list=True)
            }
            
            sas_permissions = permission_map.get(permissions, BlobSasPermissions(read=True))
            
            # SAS 토큰 생성
            sas_token = generate_blob_sas(
                account_name=self.account_name,
                container_name=self.container_name,
                blob_name=blob_name,
                account_key=self.account_key,
                permission=sas_permissions,
                expiry=datetime.utcnow() + timedelta(hours=expiration_hours)
            )
            
            # 전체 URL 생성
            blob_url = f"https://{self.account_name}.blob.core.windows.net/{self.container_name}/{blob_name}"
            sas_url = f"{blob_url}?{sas_token}"
            
            logger.debug(f"Generated SAS URL for Azure blob: {blob_name}")
            return sas_url
            
        except Exception as e:
            logger.error(f"Azure SAS URL generation error: {e}")
            raise
    
    async def snapshot_blob(self, path: str) -> str:
        """Azure 블롭 스냅샷 생성"""
        try:
            container_client = await self._get_container_client()
            blob_name = self._get_blob_name(path)
            
            blob_client = container_client.get_blob_client(blob_name)
            snapshot = await blob_client.create_snapshot()
            
            snapshot_time = snapshot['snapshot']
            logger.debug(f"Created Azure blob snapshot: {blob_name} -> {snapshot_time}")
            
            return snapshot_time
            
        except AzureError as e:
            logger.error(f"Azure snapshot creation error: {e}")
            raise
    
    async def list_snapshots(self, path: str) -> List[str]:
        """Azure 블롭 스냅샷 목록 조회"""
        try:
            container_client = await self._get_container_client()
            blob_name = self._get_blob_name(path)
            
            snapshots = []
            async for blob in container_client.list_blobs(name_starts_with=blob_name, include=['snapshots']):
                if blob.snapshot:
                    snapshots.append(blob.snapshot)
            
            return snapshots
            
        except AzureError as e:
            logger.error(f"Azure list snapshots error: {e}")
            raise
    
    async def get_blob_service_stats(self) -> dict:
        """Azure Blob 서비스 통계 조회"""
        try:
            service_client = await self._get_service_client()
            
            stats = await service_client.get_service_stats()
            
            return {
                'geo_replication': {
                    'status': stats.geo_replication.status,
                    'last_sync_time': stats.geo_replication.last_sync_time.isoformat() if stats.geo_replication.last_sync_time else None
                }
            }
            
        except AzureError as e:
            logger.error(f"Azure service stats error: {e}")
            raise
    
    async def __aenter__(self):
        """비동기 컨텍스트 매니저 진입"""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """비동기 컨텍스트 매니저 종료"""
        if self._service_client:
            await self._service_client.close()

# 사용 예제
"""
# Azure Blob Storage 소스 생성
azure_source = SourceAzureBlob(params={
    'storage_config': {
        'bucket_name': 'my-data-container',  # container_name으로 사용됨
        'multipart_threshold': 64 * 1024 * 1024,  # 64MB
        'multipart_chunksize': 16 * 1024 * 1024,  # 16MB
        'retry_attempts': 3
    },
    'account_name': 'mystorageaccount',
    'account_key': 'your-account-key-here',
    # 또는 connection_string 사용
    # 'connection_string': 'DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net',
    'operation': 'read',
    'path': 'data/sales/monthly_sales.csv'
})

# 데이터 노드와 함께 사용
azure_node = CommonNode(
    element="azure_data",
    process=azure_source
)

result = await azure_node.get_data()

# Azure 쓰기
azure_write = SourceAzureBlob(params={
    'storage_config': {
        'bucket_name': 'my-processed-data',
    },
    'account_name': 'mystorageaccount',
    'account_key': 'your-account-key-here',
    'operation': 'write',
    'path': 'processed/results.parquet'
})

# 이전 노드 결과를 Azure에 저장
save_node = CommonNode(
    azure_node,
    element="save_to_azure",
    process=azure_write
)

# 대용량 파일 스트림 처리
async def process_large_azure_file():
    azure_stream = SourceAzureBlob(params={
        'storage_config': {
            'bucket_name': 'my-large-files',
            'chunk_size': 1024 * 1024  # 1MB 청크
        },
        'account_name': 'mystorageaccount',
        'account_key': 'your-account-key-here'
    })
    
    async for chunk in azure_stream.read_stream('large_data/big_file.bin'):
        # 청크별 처리
        process_chunk(chunk)

# SAS URL 생성 (임시 접근)
async def create_temp_access():
    azure_source = SourceAzureBlob(params={
        'storage_config': {'bucket_name': 'my-private-container'},
        'account_name': 'mystorageaccount',
        'account_key': 'your-account-key-here'
    })
    
    sas_url = await azure_source.generate_sas_url(
        'reports/confidential_report.pdf',
        expiration_hours=2,
        permissions='r'  # 읽기 전용
    )
    print(f"Temporary access URL (2 hours): {sas_url}")

# 블롭 스냅샷 생성
async def create_backup():
    azure_source = SourceAzureBlob(params={
        'storage_config': {'bucket_name': 'my-important-data'},
        'account_name': 'mystorageaccount',
        'account_key': 'your-account-key-here'
    })
    
    snapshot_time = await azure_source.snapshot_blob('critical/database_backup.sql')
    print(f"Backup snapshot created: {snapshot_time}")

# 액세스 계층 변경 (비용 최적화)
async def optimize_storage_costs():
    azure_source = SourceAzureBlob(params={
        'storage_config': {'bucket_name': 'my-archive-data'},
        'account_name': 'mystorageaccount',
        'account_key': 'your-account-key-here'
    })
    
    # 자주 사용하지 않는 파일을 Cool 계층으로 이동
    await azure_source.set_blob_tier('old_data/archive_2023.zip', 'Cool')
    
    # 장기 보관용 파일을 Archive 계층으로 이동
    await azure_source.set_blob_tier('backup/old_backup_2022.tar.gz', 'Archive')
"""