# minio_config.py - auto generated
from minio import Minio

def get_minio_client():
    return Minio(
        "localhost:9000",
        access_key="1zFoczK2HB6k6mgPtgFk",
        secret_key="PvsvEIsztNe8uAZQRTy66LLoduPTn5idZsv4EbgO",
        secure=False
    )
