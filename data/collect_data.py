# collect_data.py
from confluent_kafka import Consumer
from minio import Minio
import json
import csv
import io
from config.minio_config import get_minio_client

# Cấu hình Kafka Consumer
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mycode',
    'auto.offset.reset': 'earliest'
}

minio_client=get_minio_client()

bucket_name = "eventsdataset"
EXPECTED_FIELDS = [
    "event_time", "event_type", "product_id", "category_id",
    "category_code", "brand", "price", "user_id", "user_session"
]

# Tạo bucket nếu chưa tồn tại
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)
    print(f"✅ Đã tạo bucket '{bucket_name}'.")

# Tên cố định của file CSV
csv_filename = "aaaa.csv"

# Đọc dữ liệu cũ từ MinIO nếu có
def get_existing_data():
    try:
        response = minio_client.get_object(bucket_name, csv_filename)
        content = response.read().decode("utf-8").splitlines()
        return content
    except Exception:
        return []

def collect_data():
    # Khởi tạo Kafka Consumer
    consumer = Consumer(kafka_conf)
    consumer.subscribe(['json-topic'])
    print("🎧 Đang lắng nghe tin nhắn trên topic: 'json-topic'")

    # Lưu dữ liệu hiện có trong bộ đệm
    existing_rows = get_existing_data()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                continue

            try:
                # Xử lý tin nhắn Kafka
                json_data = json.loads(msg.value().decode("utf-8"))
                data = {field: json_data.get(field, "").strip() if isinstance(json_data.get(field), str) else json_data.get(field, 0) for field in EXPECTED_FIELDS}
                data["price"] = float(json_data.get("price", 0))

                # Thêm dòng mới vào buffer
                output = io.StringIO()
                writer = csv.DictWriter(output, fieldnames=EXPECTED_FIELDS)
                if not existing_rows:
                    writer.writeheader()  # Ghi header nếu lần đầu
                writer.writerow(data)
                existing_rows.append(output.getvalue().strip())

                # Tải toàn bộ dữ liệu mới lên MinIO
                final_csv = "\n".join(existing_rows)
                csv_bytes = final_csv.encode("utf-8")
                csv_buffer = io.BytesIO(csv_bytes)

                minio_client.put_object(
                    bucket_name,
                    csv_filename,
                    csv_buffer,
                    length=len(csv_bytes),
                    content_type="text/csv"
                )
                print(f"✔️ Ghi dữ liệu và cập nhật '{csv_filename}': {data}")

            except (json.JSONDecodeError, ValueError) as e:
                print(f"❌ Lỗi xử lý tin nhắn: {e}")

    except KeyboardInterrupt:
        print("\n🛑 Dừng consumer...")
    finally:
        consumer.close()
