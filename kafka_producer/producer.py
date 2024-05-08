# File: kafka_producer/app.py

from flask import Flask
from kafka import KafkaProducer
from datetime import datetime
import time

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='localhost:9092')

@app.route('/start', methods=['GET'])
def start_producing():
    for i in range(1000):
        current_time = datetime.now().isoformat()
        producer.send('comments', current_time.encode('utf-8'))
        time.sleep(1)  # wait for 1 second
    return 'Started sending current time to Kafka topic'

if __name__ == '__main__':
    start_producing()
    app.run(debug=True)