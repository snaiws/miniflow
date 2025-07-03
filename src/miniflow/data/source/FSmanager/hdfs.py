# data/source/storage/hdfs.py
import asyncio
import json
import pickle
from typing import Any, List, Optional, AsyncIterator
import tempfile
import os
import logging

import pandas as pd
from hdfs import InsecureClient, Config
from hdfs.util import HdfsError

from .base import FileSystemSource, StorageMetadata

logger = logging.getLogger(__name__)

class SourceHDFS(FileSystemSource):
    """Hadoop 분산 파일 시스템 (HDFS) 소스"""
    
    def __init__(self, params: dict):
        super().__init__(params)
        self._client = None
        self._executor = None
    
    async def _get_client(self):
        """HDFS 클라이언트 가져오기 (지연 초기화)"""
        if self._client is None:
            # HDFS 설정에서 URL 가져오기
            namenode_url = getattr(self.config, 'namenode_url', 'http://localhost:9870')
            username = getattr(self.config, 'username', 'hdfs')
            
            # 비동기 실행을 위한 스레드 풀
            if self._executor is None:
                self._executor = asyncio.get_event_loop()
            
            self._client = InsecureClient(
                url=namenode_url,
                user=username,
                timeout=self.config.connection_timeout,
                session_timeout=self.config.read_timeout
            )
        
        return self._client
    
    def _get_full_path(self, path: str) -> str:
        """전체 HDFS 경로 생성"""
        if self.config.base_path:
            if not path.startswith('/'):
                return f"{self.config.base_path.rstrip('/')}/{path}"
            else:
                return f"{self.config.base_path.rstrip('')}{path}"
        return path
    
    async def read(self, path: str) -> Any:
        """HDFS 파일 읽기"""
        try:
            client = await self._get_client()
            hdfs_path = self._get_full_path(path)
            
            logger.debug(f"Reading HDFS file: {hdfs_path}")
            
            # HDFS는 동기 API이므로 스레드 풀에서 실행
            content = await asyncio.to_thread(self._read_hdfs_file, client, hdfs_path)
            
            # 파일 확장자에 따라 자동 변환
            if path.endswith('.json'):
                return json.loads(content.decode('utf-8'))
            elif path.endswith('.csv'):
                import io
                return pd.read_csv(io.BytesIO(content))
            elif path.endswith('.parquet'):
                # Parquet은 임시 파일을 통해 처리
                return await self._read_parquet_from_hdfs(client, hdfs_path)
            elif path.endswith('.pkl') or path.endswith('.pickle'):
                return pickle.loads(content)
            elif path.endswith(('.txt', '.log')):
                return content.decode('utf-8')
            else:
                return content  # 바이너리 그대로 반환
                
        except HdfsError as e:
            if 'File does not exist' in str(e):
                raise FileNotFoundError(f"HDFS file not found: {hdfs_path}")
            else:
                logger.error(f"HDFS read error: {e}")
                raise
        except Exception as e:
            logger.error(f"HDFS read error for {path}: {e}")
            raise
    
    def _read_hdfs_file(self, client, hdfs_path: str) -> bytes:
        """HDFS 파일 읽기 (동기)"""
        with client.read(hdfs_path) as reader:
            return reader.read()
    
    async def _read_parquet_from_hdfs(self, client, hdfs_path: str) -> pd.DataFrame:
        """HDFS에서 Parquet 파일 읽기"""
        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp_file:
            try:
                # HDFS에서 임시 파일로 다운로드
                await asyncio.to_thread(client.download, hdfs_path, tmp_file.name, overwrite=True)
                
                # 임시 파일에서 Parquet 읽기
                df = await asyncio.to_thread(pd.read_parquet, tmp_file.name)
                return df
            finally:
                # 임시 파일 삭제
                if os.path.exists(tmp_file.name):
                    os.unlink(tmp_file.name)
    
    async def write(self, path: str, data: Any) -> bool:
        """HDFS 파일 쓰기"""
        try:
            client = await self._get_client()
            hdfs_path = self._get_full_path(path)
            
            # 디렉토리 생성
            if self.config.create_dirs:
                parent_dir = '/'.join(hdfs_path.split('/')[:-1])
                if parent_dir:
                    await asyncio.to_thread(client.makedirs, parent_dir)
            
            # 데이터 타입에 따라 변환
            if isinstance(data, dict) or isinstance(data, list):
                content = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')
            elif isinstance(data, pd.DataFrame):
                if path.endswith('.csv'):
                    content = data.to_csv(index=False).encode('utf-8')
                elif path.endswith('.parquet'):
                    return await self._write_parquet_to_hdfs(client, hdfs_path, data)
                else:
                    content = data.to_json(orient='records').encode('utf-8')
            elif isinstance(data, str):
                content = data.encode('utf-8')
            elif isinstance(data, bytes):
                content = data
            else:
                # pickle로 직렬화
                content = pickle.dumps(data)
            
            logger.debug(f"Writing HDFS file: {hdfs_path} ({len(content)} bytes)")
            
            # HDFS에 쓰기
            await asyncio.to_thread(self._write_hdfs_file, client, hdfs_path, content)
            
            return True
            
        except Exception as e:
            logger.error(f"HDFS write error for {path}: {e}")
            raise
    
    def _write_hdfs_file(self, client, hdfs_path: str, content: bytes):
        """HDFS 파일 쓰기 (동기)"""
        with client.write(hdfs_path, overwrite=True) as writer:
            writer.write(content)
    
    async def _write_parquet_to_hdfs(self, client, hdfs_path: str, data: pd.DataFrame) -> bool:
        """HDFS에 Parquet 파일 쓰기"""
        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp_file:
            try:
                # 임시 파일에 Parquet 저장
                await asyncio.to_thread(data.to_parquet, tmp_file.name)
                
                # 임시 파일을 HDFS로 업로드
                await asyncio.to_thread(client.upload, hdfs_path, tmp_file.name, overwrite=True)
                
                return True
            finally:
                # 임시 파일 삭제
                if os.path.exists(tmp_file.name):
                    os.unlink(tmp_file.name)
    
    async def list_objects(self, path: str = "", pattern: str = "*", recursive: bool = False) -> List[str]:
        """HDFS 파일 목록 조회"""
        try:
            client = await self._get_client()
            hdfs_path = self._get_full_path(path) if path else '/'
            
            files = []
            
            if recursive:
                # 재귀적 파일 목록
                file_list = await asyncio.to_thread(client.list, hdfs_path, status=True)
                for file_path, status in file_list:
                    if status['type'] == 'FILE':
                        full_path = f"{hdfs_path.rstrip('/')}/{file_path}"
                        if self._match_pattern(file_path, pattern):
                            files.append(full_path)
                    elif status['type'] == 'DIRECTORY' and recursive:
                        # 재귀적으로 하위 디렉토리 탐색
                        sub_path = f"{hdfs_path.rstrip('/')}/{file_path}"
                        sub_files = await self.list_objects(sub_path, pattern, recursive=True)
                        files.extend(sub_files)
            else:
                # 현재 디렉토리만
                file_list = await asyncio.to_thread(client.list, hdfs_path, status=True)
                for file_path, status in file_list:
                    if status['type'] == 'FILE':
                        full_path = f"{hdfs_path.rstrip('/')}/{file_path}"
                        if self._match_pattern(file_path, pattern):
                            files.append(full_path)
            
            return files
            
        except HdfsError as e:
            if 'File does not exist' in str(e):
                logger.warning(f"HDFS directory not found: {hdfs_path}")
                return []
            else:
                logger.error(f"HDFS list error: {e}")
                raise
    
    def _match_pattern(self, filename: str, pattern: str) -> bool:
        """간단한 패턴 매칭"""
        import fnmatch
        return fnmatch.fnmatch(filename, pattern)
    
    async def delete(self, path: str) -> bool:
        """HDFS 파일 삭제"""
        try:
            client = await self._get_client()
            hdfs_path = self._get_full_path(path)
            
            result = await asyncio.to_thread(client.delete, hdfs_path)
            
            if result:
                logger.debug(f"Deleted HDFS file: {hdfs_path}")
            else:
                logger.warning(f"HDFS file not found for deletion: {hdfs_path}")
            
            return result
            
        except Exception as e:
            logger.error(f"HDFS delete error: {e}")
            raise
    
    async def exists(self, path: str) -> bool:
        """HDFS 파일 존재 확인"""
        try:
            client = await self._get_client()
            hdfs_path = self._get_full_path(path)
            
            # HDFS 클라이언트는 존재 확인을 위해 status를 호출
            try:
                await asyncio.to_thread(client.status, hdfs_path)
                return True
            except HdfsError as e:
                if 'File does not exist' in str(e):
                    return False
                else:
                    raise
            
        except Exception as e:
            logger.error(f"HDFS exists check error: {e}")
            raise
    
    async def get_metadata(self, path: str) -> StorageMetadata:
        """HDFS 파일 메타데이터 조회"""
        try:
            client = await self._get_client()
            hdfs_path = self._get_full_path(path)
            
            status = await asyncio.to_thread(client.status, hdfs_path)
            
            return StorageMetadata(
                size=status.get('length'),
                created_time=None,  # HDFS는 생성 시간을 제공하지 않음
                modified_time=status.get('modificationTime'),
                content_type=None,  # HDFS는 content type을 제공하지 않음
                etag=None,
                custom_metadata={
                    'owner': status.get('owner'),
                    'group': status.get('group'),
                    'permission': status.get('permission'),
                    'replication': status.get('replication'),
                    'block_size': status.get('blockSize')
                }
            )
            
        except HdfsError as e:
            if 'File does not exist' in str(e):
                raise FileNotFoundError(f"HDFS file not found: {hdfs_path}")
            else:
                logger.error(f"HDFS metadata error: {e}")
                raise
    
    async def copy(self, source_path: str, dest_path: str) -> bool:
        """HDFS 파일 복사"""
        try:
            client = await self._get_client()
            source_hdfs_path = self._get_full_path(source_path)
            dest_hdfs_path = self._get_full_path(dest_path)
            
            # HDFS는 직접 복사 기능이 없으므로 읽기-쓰기로 구현
            content = await asyncio.to_thread(self._read_hdfs_file, client, source_hdfs_path)
            await asyncio.to_thread(self._write_hdfs_file, client, dest_hdfs_path, content)
            
            logger.debug(f"Copied HDFS file: {source_hdfs_path} -> {dest_hdfs_path}")
            return True
            
        except Exception as e:
            logger.error(f"HDFS copy error: {e}")
            raise
    
    async def read_stream(self, path: str) -> AsyncIterator[bytes]:
        """HDFS 파일 스트림 읽기"""
        try:
            client = await self._get_client()
            hdfs_path = self._get_full_path(path)
            
            def stream_reader():
                with client.read(hdfs_path) as reader:
                    while True:
                        chunk = reader.read(self.config.chunk_size)
                        if not chunk:
                            break
                        yield chunk
            
            # 동기 제너레이터를 비동기로 변환
            for chunk in await asyncio.to_thread(lambda: list(stream_reader())):
                yield chunk
                
        except Exception as e:
            logger.error(f"HDFS stream read error for {path}: {e}")
            raise
    
    async def write_stream(self, path: str, data_stream: AsyncIterator[bytes]) -> bool:
        """HDFS 파일 스트림 쓰기"""
        try:
            client = await self._get_client()
            hdfs_path = self._get_full_path(path)
            
            # 디렉토리 생성
            if self.config.create_dirs:
                parent_dir = '/'.join(hdfs_path.split('/')[:-1])
                if parent_dir:
                    await asyncio.to_thread(client.makedirs, parent_dir)
            
            def stream_writer():
                with client.write(hdfs_path, overwrite=True) as writer:
                    async def write_chunks():
                        async for chunk in data_stream:
                            writer.write(chunk)
                    
                    # 비동기 스트림을 동기 컨텍스트에서 처리
                    import asyncio
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        loop.run_until_complete(write_chunks())
                    finally:
                        loop.close()
            
            await asyncio.to_thread(stream_writer)
            
            logger.debug(f"Stream written to HDFS: {hdfs_path}")
            return True
            
        except Exception as e:
            logger.error(f"HDFS stream write error for {path}: {e}")
            raise
    
    async def set_replication(self, path: str, replication: int) -> bool:
        """HDFS 파일 복제 수 설정"""
        try:
            client = await self._get_client()
            hdfs_path = self._get_full_path(path)
            
            await asyncio.to_thread(client.set_replication, hdfs_path, replication)
            
            logger.debug(f"Set HDFS replication: {hdfs_path} -> {replication}")
            return True
            
        except Exception as e:
            logger.error(f"HDFS set replication error: {e}")
            raise
    
    async def set_permission(self, path: str, permission: str) -> bool:
        """HDFS 파일 권한 설정"""
        try:
            client = await self._get_client()
            hdfs_path = self._get_full_path(path)
            
            await asyncio.to_thread(client.set_permission, hdfs_path, permission)
            
            logger.debug(f"Set HDFS permission: {hdfs_path} -> {permission}")
            return True
            
        except Exception as e:
            logger.error(f"HDFS set permission error: {e}")
            raise
    
    async def get_disk_usage(self, path: str = "/") -> dict:
        """HDFS 디스크 사용량 조회"""
        try:
            client = await self._get_client()
            hdfs_path = self._get_full_path(path)
            
            usage = await asyncio.to_thread(client.content, hdfs_path, strict=False)
            
            return {
                'quota': usage.get('quota', -1),
                'space_consumed': usage.get('spaceConsumed', 0),
                'space_quota': usage.get('spaceQuota', -1),
                'file_count': usage.get('fileCount', 0),
                'directory_count': usage.get('directoryCount', 0)
            }
            
        except Exception as e:
            logger.error(f"HDFS disk usage error: {e}")
            raise

# 사용 예제
"""
# HDFS 소스 생성
hdfs_source = SourceHDFS(params={
    'storage_config': {
        'namenode_url': 'http://namenode:9870',
        'username': 'hadoop',
        'base_path': '/user/data',
        'create_dirs': True,
        'connection_timeout': 30.0,
        'chunk_size': 65536  # 64KB
    },
    'operation': 'read',
    'path': 'logs/access.log'
})

# 데이터 노드와 함께 사용
hdfs_node = CommonNode(
    element="hdfs_data",
    process=hdfs_source
)

# Parquet 파일 처리
parquet_source = SourceHDFS(params={
    'storage_config': {
        'namenode_url': 'http://namenode:9870',
        'username': 'hadoop',
        'base_path': '/warehouse',
        'create_dirs': True
    },
    'operation': 'read',
    'path': 'sales/2024/sales_data.parquet'
})

parquet_node = CommonNode(
    element="sales_data",
    process=parquet_source
)

# 대용량 파일 스트림 처리
async def process_large_file():
    hdfs_stream = SourceHDFS(params={
        'storage_config': {
            'namenode_url': 'http://namenode:9870',
            'username': 'hadoop'
        }
    })
    
    async for chunk in hdfs_stream.read_stream('/large_data/big_file.txt'):
        # 청크별 처리
        process_chunk(chunk)

# HDFS 파일 복제 수 설정
async def set_file_replication():
    hdfs_source = SourceHDFS(params={
        'storage_config': {
            'namenode_url': 'http://namenode:9870',
            'username': 'hadoop'
        }
    })
    
    await hdfs_source.set_replication('/important/data.csv', 5)  # 복제 수 5로 설정
"""