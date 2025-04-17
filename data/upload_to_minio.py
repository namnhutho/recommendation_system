from io import BytesIO
from minio.error import S3Error
from config.minio_config import get_minio_client
from tqdm import tqdm

def upload_df_to_minio(df, bucket_name, object_name):
    client = get_minio_client()

    # Tạo bucket nếu chưa tồn tại
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Ghi DataFrame thành CSV vào buffer
    buffer = BytesIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    try:
        client.put_object(
            bucket_name,
            object_name,
            buffer,
            length=buffer.getbuffer().nbytes,
            content_type="text/csv"
        )
        print(f"✅ Đã upload thành công: s3://{bucket_name}/{object_name}")
    except S3Error as e:
        print(f"❌ Lỗi khi upload lên MinIO: {e}")



class TqdmReadableStream:
    def __init__(self, buffer, desc="Uploading"):
        self.buffer = buffer
        self.total = len(buffer.getbuffer())
        self.tqdm_bar = tqdm(total=self.total, unit='B', unit_scale=True, desc=desc)

    def read(self, n=-1):
        chunk = self.buffer.read(n)
        self.tqdm_bar.update(len(chunk))
        return chunk

    def seek(self, offset, whence=0):
        return self.buffer.seek(offset, whence)

    def tell(self):
        return self.buffer.tell()

    def __len__(self):
        return self.total

    def close(self):
        self.tqdm_bar.close()

def upload_df_to_minio_as_parquet(df, bucket_name, object_name):
    client = get_minio_client()

    # Tạo bucket nếu chưa tồn tại
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Ghi DataFrame thành parquet vào buffer
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    reader = TqdmReadableStream(buffer)

    try:
        client.put_object(
            bucket_name,
            object_name,
            reader,
            length=len(reader),
            content_type="application/octet-stream"
        )
        reader.close()
        print("✅ Upload thành công!")
    except S3Error as e:
        reader.close()
        print(f"❌ Lỗi khi upload lên MinIO: {e}")
