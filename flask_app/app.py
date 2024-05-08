from flask import Flask, request, jsonify
from kafka import KafkaConsumer, KafkaProducer
import threading
import json
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # <- Add this line to allow CORS

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

comments = []

def consume_messages():
    consumer = KafkaConsumer('comments', 
                             bootstrap_servers='localhost:9092', 
                             group_id='comment_group003',  # Use the same group_id
                             auto_offset_reset='earliest',
                             enable_auto_commit=True)
    for message in consumer:
        comment = message.value.decode('utf-8')
        comments.append(comment)

@app.route('/comment', methods=['POST'])
def comment():
    data = request.get_json()
    comment = data['comment']
    producer.send('comments', comment)
    comments.append(comment)
    return 'Comment sent to Kafka topic'

@app.route('/comment', methods=['GET'])
def get_comments():
    global comments
    return jsonify({'comments': comments})

if __name__ == '__main__':
    consumer_thread = threading.Thread(target=consume_messages)
    consumer_thread.start()
    app.run(debug=True)
