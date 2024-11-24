from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
import json

# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='my_consumer_group_3'   # Ідентифікатор групи споживачів
)
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Назва топіку
topic_names = ['building_sensors_rudyi',
               'temperature_alerts_rudyi', 'humidity_alerts_rudyi']

# Підписка на тему
consumer.subscribe([topic_names[0]])

print(f"Subscribed to topic '{topic_names[0]}'")

# Обробка повідомлень з топіку
try:
    for message in consumer:
        print(
            f"Received message: {message.value} with key: {message.key}, partition {message.partition}")
        
        if message.value['temperature'] > 40:
            alert = message.value
            alert['type'] = 'temperature greater than 40 degrees'
            producer.send(topic_names[1], key=message.key, value=alert)
        
        if message.value['humidity'] > 80:
            alert = message.value
            alert['type'] = 'humidity greater than 80%'
            producer.send(topic_names[2], key=message.key, value=alert)
        
        if message.value['humidity'] < 20:
            alert = message.value
            alert['type'] = 'humidity less than 20%'
            producer.send(topic_names[2], key=message.key, value=alert)
        
        producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
except KeyboardInterrupt:
    print("Script stopped manually.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()
    producer.close()  # Закриття consumer
