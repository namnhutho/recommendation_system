from flask import Blueprint, request, jsonify
from kafka import KafkaProducer
import json

kafka_bp = Blueprint('kafka_bp', __name__)
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@kafka_bp.route('/send-kafka', methods=['POST'])
def send_to_kafka():
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No JSON provided'}), 400

    producer.send('json-topic', value=data)
    producer.flush()
    return jsonify({'status': 'Message sent to Kafka ✅'}), 200
