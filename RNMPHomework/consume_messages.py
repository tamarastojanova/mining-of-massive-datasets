from kafka import KafkaConsumer

bootstrap_servers = 'localhost:9092'
topic = 'health_data_predicted'

consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='latest',
                         enable_auto_commit=False)
try:
    for message in consumer:
        print(f"Received message: {message.value.decode('utf-8')}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
