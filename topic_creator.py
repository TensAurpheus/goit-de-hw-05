from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Визначення нового топіку
my_name = "oleksiy"
topic_names = ['building_sensors_rudyi', 'temperature_alerts_rudyi', 'humidity_alerts_rudyi']
num_partitions = 2
replication_factor = 1

topics = [NewTopic(name=name, num_partitions=num_partitions, replication_factor=replication_factor) for name in topic_names]

# Створення нового топіку
try:
    admin_client.create_topics(new_topics=topics, validate_only=False)
    print(f"Topics {topic_names} created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

# Перевіряємо список існуючих топіків
[print(topic) for topic in admin_client.list_topics() if "rudyi" in topic]

# Закриття зв'язку з клієнтом
admin_client.close()
