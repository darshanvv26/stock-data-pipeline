"""
MinIO Client Wrapper for Data Lake Operations

This module provides a wrapper around the MinIO Python SDK for interacting
with the data lake. It supports operations for the Bronze, Silver, and Gold
layers of the medallion architecture.
"""

import os
import json
import logging
from io import BytesIO
from datetime import datetime
from typing import Optional, Union, List

from minio import Minio
from minio.error import S3Error

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Bucket names for medallion architecture
BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"
GOLD_BUCKET = "gold"


class MinioDataLake:
    """
    MinIO Data Lake client for medallion architecture operations.
    
    Supports uploading, downloading, and listing objects across
    Bronze, Silver, and Gold layers.
    """
    
    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        secure: bool = False
    ):
        """
        Initialize MinIO client.
        
        Args:
            endpoint: MinIO server endpoint (default from env)
            access_key: MinIO access key (default from env)
            secret_key: MinIO secret key (default from env)
            secure: Use HTTPS (default False for local development)
        """
        self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self.access_key = access_key or os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = secret_key or os.getenv("MINIO_SECRET_KEY", "minioadmin")
        
        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=secure
        )
        
        logger.info(f"MinIO client initialized for endpoint: {self.endpoint}")
    
    def ensure_buckets_exist(self) -> None:
        """Create Bronze, Silver, and Gold buckets if they don't exist."""
        buckets = [BRONZE_BUCKET, SILVER_BUCKET, GOLD_BUCKET]
        
        for bucket in buckets:
            try:
                if not self.client.bucket_exists(bucket):
                    self.client.make_bucket(bucket)
                    logger.info(f"Created bucket: {bucket}")
                else:
                    logger.info(f"Bucket already exists: {bucket}")
            except S3Error as e:
                logger.error(f"Error creating bucket {bucket}: {e}")
                raise
    
    def upload_json(
        self,
        bucket: str,
        object_name: str,
        data: Union[dict, list],
        metadata: Optional[dict] = None
    ) -> str:
        """
        Upload JSON data to a bucket.
        
        Args:
            bucket: Target bucket name
            object_name: Object name/path in the bucket
            data: JSON-serializable data
            metadata: Optional metadata for the object
            
        Returns:
            Object path (bucket/object_name)
        """
        try:
            json_bytes = json.dumps(data, indent=2, default=str).encode('utf-8')
            data_stream = BytesIO(json_bytes)
            
            self.client.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=data_stream,
                length=len(json_bytes),
                content_type='application/json',
                metadata=metadata
            )
            
            path = f"{bucket}/{object_name}"
            logger.info(f"Uploaded JSON to: {path}")
            return path
            
        except S3Error as e:
            logger.error(f"Error uploading JSON to {bucket}/{object_name}: {e}")
            raise
    
    def upload_parquet(
        self,
        bucket: str,
        object_name: str,
        data: bytes,
        metadata: Optional[dict] = None
    ) -> str:
        """
        Upload Parquet data to a bucket.
        
        Args:
            bucket: Target bucket name
            object_name: Object name/path in the bucket
            data: Parquet file bytes
            metadata: Optional metadata for the object
            
        Returns:
            Object path (bucket/object_name)
        """
        try:
            data_stream = BytesIO(data)
            
            self.client.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=data_stream,
                length=len(data),
                content_type='application/octet-stream',
                metadata=metadata
            )
            
            path = f"{bucket}/{object_name}"
            logger.info(f"Uploaded Parquet to: {path}")
            return path
            
        except S3Error as e:
            logger.error(f"Error uploading Parquet to {bucket}/{object_name}: {e}")
            raise
    
    def download_json(self, bucket: str, object_name: str) -> Union[dict, list]:
        """
        Download and parse JSON data from a bucket.
        
        Args:
            bucket: Source bucket name
            object_name: Object name/path in the bucket
            
        Returns:
            Parsed JSON data
        """
        try:
            response = self.client.get_object(bucket, object_name)
            data = json.loads(response.read().decode('utf-8'))
            response.close()
            response.release_conn()
            
            logger.info(f"Downloaded JSON from: {bucket}/{object_name}")
            return data
            
        except S3Error as e:
            logger.error(f"Error downloading JSON from {bucket}/{object_name}: {e}")
            raise
    
    def download_parquet(self, bucket: str, object_name: str) -> bytes:
        """
        Download Parquet data from a bucket.
        
        Args:
            bucket: Source bucket name
            object_name: Object name/path in the bucket
            
        Returns:
            Parquet file bytes
        """
        try:
            response = self.client.get_object(bucket, object_name)
            data = response.read()
            response.close()
            response.release_conn()
            
            logger.info(f"Downloaded Parquet from: {bucket}/{object_name}")
            return data
            
        except S3Error as e:
            logger.error(f"Error downloading Parquet from {bucket}/{object_name}: {e}")
            raise
    
    def list_objects(
        self,
        bucket: str,
        prefix: Optional[str] = None,
        recursive: bool = True
    ) -> List[str]:
        """
        List objects in a bucket.
        
        Args:
            bucket: Bucket name
            prefix: Optional prefix to filter objects
            recursive: List recursively (default True)
            
        Returns:
            List of object names
        """
        try:
            objects = self.client.list_objects(
                bucket,
                prefix=prefix,
                recursive=recursive
            )
            object_names = [obj.object_name for obj in objects]
            logger.info(f"Listed {len(object_names)} objects in {bucket}/{prefix or ''}")
            return object_names
            
        except S3Error as e:
            logger.error(f"Error listing objects in {bucket}: {e}")
            raise
    
    def object_exists(self, bucket: str, object_name: str) -> bool:
        """Check if an object exists in a bucket."""
        try:
            self.client.stat_object(bucket, object_name)
            return True
        except S3Error:
            return False
    
    # Convenience methods for medallion layers
    
    def upload_to_bronze(
        self,
        symbol: str,
        date: str,
        data: Union[dict, list]
    ) -> str:
        """Upload raw stock data to Bronze layer."""
        object_name = f"stocks/{symbol}/{date}.json"
        return self.upload_json(BRONZE_BUCKET, object_name, data)
    
    def upload_to_silver(
        self,
        symbol: str,
        date: str,
        parquet_data: bytes
    ) -> str:
        """Upload cleaned data to Silver layer."""
        object_name = f"stocks/{symbol}/{date}.parquet"
        return self.upload_parquet(SILVER_BUCKET, object_name, parquet_data)
    
    def upload_to_gold(
        self,
        symbol: str,
        date: str,
        parquet_data: bytes,
        kpi_type: str = "daily"
    ) -> str:
        """Upload KPI data to Gold layer."""
        object_name = f"kpis/{kpi_type}/{symbol}/{date}.parquet"
        return self.upload_parquet(GOLD_BUCKET, object_name, parquet_data)
    
    def get_bronze_files(self, symbol: Optional[str] = None) -> List[str]:
        """List files in Bronze layer, optionally filtered by symbol."""
        prefix = f"stocks/{symbol}/" if symbol else "stocks/"
        return self.list_objects(BRONZE_BUCKET, prefix)
    
    def get_silver_files(self, symbol: Optional[str] = None) -> List[str]:
        """List files in Silver layer, optionally filtered by symbol."""
        prefix = f"stocks/{symbol}/" if symbol else "stocks/"
        return self.list_objects(SILVER_BUCKET, prefix)
    
    def get_gold_files(self, kpi_type: str = "daily") -> List[str]:
        """List files in Gold layer."""
        prefix = f"kpis/{kpi_type}/"
        return self.list_objects(GOLD_BUCKET, prefix)


# Singleton instance for easy importing
_datalake_instance = None


def get_datalake() -> MinioDataLake:
    """Get or create the singleton MinIO data lake instance."""
    global _datalake_instance
    if _datalake_instance is None:
        _datalake_instance = MinioDataLake()
    return _datalake_instance
