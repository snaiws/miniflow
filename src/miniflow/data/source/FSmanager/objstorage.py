from typing import Dict, Any, Optional, ClassVar
import asyncio
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.client import Config  
from botocore.exceptions import ClientError  

from .base import Source

class SourceOS(Source):
    _instances: ClassVar[Dict[str, 'SourceOS']] = {}
    _lock = asyncio.Lock()
    _executor = ThreadPoolExecutor()
    
    def __new__(cls, endpoint_url, *args, **kwargs):
        # base_url을 키로 사용하여 인스턴스 관리
        if endpoint_url not in cls._instances:
            cls._instances[endpoint_url] = super(SourceOS, cls).__new__(cls)
        return cls._instances[endpoint_url]
    
    def __init__(self, endpoint_url, aws_access_key_id, aws_secret_access_key, region_name):  
        # Check if this instance has already been initialized
        if not hasattr(self, '_initialized'):
            self.__endpoint_url = endpoint_url
            self.__aws_access_key_id = aws_access_key_id
            self.__aws_secret_access_key = aws_secret_access_key
            self.__signature_version = 's3v4'
            self.__region_name = region_name
            self.client = None
            self.connect()
            self._initialized = True

    def connect(self):
        self.client = boto3.client(  
            's3',  
            endpoint_url = self.__endpoint_url,  
            aws_access_key_id = self.__aws_access_key_id,  
            aws_secret_access_key = self.__aws_secret_access_key,  
            config = Config(signature_version = self.__signature_version),  
            region_name = self.__region_name  
        )
    
    async def ensure_bucket_exists(self, bucket_name: str) -> bool:  
        """버킷이 없으면 생성"""  
        try:
            # Execute the blocking S3 operation in ThreadPoolExecutor
            await asyncio.get_event_loop().run_in_executor(
                self._executor, 
                lambda: self.client.head_bucket(Bucket=bucket_name)
            )
        except ClientError as e:
            # Check if the error is because the bucket doesn't exist
            if e.response['Error']['Code'] == '404':
                # Create bucket in the executor
                await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    lambda: self.client.create_bucket(Bucket=bucket_name)
                )
            else:
                # Re-raise other errors
                raise e
        return True
    
    async def upload_file(self, bucket: str, local_path: str, object_path: str) -> bool:  
        """파일 업로드"""  
        # First ensure the bucket exists
        await self.ensure_bucket_exists(bucket)
        
        # Execute upload file in ThreadPoolExecutor
        await asyncio.get_event_loop().run_in_executor(
            self._executor,
            lambda: self.client.upload_file(local_path, bucket, object_path)
        )
        return True
    
    async def download_file(self, bucket: str, local_path: str, object_path: str) -> bool:  
        """파일 다운로드"""  
        # Execute download file in ThreadPoolExecutor
        await asyncio.get_event_loop().run_in_executor(
            self._executor,
            lambda: self.client.download_file(bucket, object_path, local_path)
        )
        return True

    def __call__(self, *data, mode):
        if mode == "upload":
            return self.upload(*data)
        elif mode == "download":
            return self.download(*data)
        

if __name__ == "__main__":
    import os  

    from dotenv import load_dotenv  
    load_dotenv(verbose = False)
    
    kwargs = {
        "endpoint_url" : os.getenv('MINIO_ENDPOINT'),
        "aws_access_key_id" : os.getenv('MINIO_ACCESS_KEY'),
        "aws_secret_access_key" : os.getenv('MINIO_SECRET_KEY'),
        "region_name" : os.getenv('MINIO_REGION', 'us-east-1')  
    }
    object_storage = OSManager(**kwargs)

    print(object_storage.check_bucket_exists('tada'))