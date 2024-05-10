import firebase_admin
from threading import Thread
from firebase_admin import credentials, db
from flask import Flask, request
from flask_cors import CORS
from kafka import KafkaProducer, KafkaConsumer
from pyspark.sql import SparkSession
import json

app = Flask(__name__)
CORS(app)

# Đường dẫn đến tệp firebase-adminsdk.json
cred = credentials.Certificate("./firebase-adminsdk.json")

# Khởi tạo Firebase
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://classify-comments-default-rtdb.asia-southeast1.firebasedatabase.app'
})

# Firebase database reference
comments_ref = db.reference('comments')

# Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Spark session
spark = SparkSession.builder \
    .appName("CommentProcessor") \
    .getOrCreate()

# Danh sách từ toxic
toxic_words = [
    "idiot",
    "stupid",
    "fool",
    "moron",
    "dumb",
    "retard",
    "asshole",
    "bitch",
    "fuck",
    "shit",
    "bastard",
    "motherfucker",
    "douchebag",
    "cunt",
    "faggot",
    "nigger",
    "whore",
    "slut"
]

def is_toxic(comment):
    # Kiểm tra xem comment có chứa từ toxic không
    for word in toxic_words:
        if word in comment:
            return True
    return False

def process_comments():
    # Kafka consumer
    consumer = KafkaConsumer('comments', 
                             bootstrap_servers='localhost:9092', 
                             group_id='comment_group002',  
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    
    for message in consumer:
        # Xử lý comment bằng Spark
        comment = message.value['comment']
        time = message.value['time']
        label = "toxic" if is_toxic(comment) else "non-toxic"
        
        # Lưu comment lên Firebase
        comments_ref.push().set({'comment': comment, 'label': label, 'time': time})

# Flask route để nhận comment từ người dùng
@app.route('/comment', methods=['POST'])
def comment():
    data = request.get_json()
    comment = data['comment']
    time = data['time']
    
    # Gửi comment tới Kafka
    producer.send('comments', {'comment': comment, 'time': time})
    
    return 'Comment sent to Kafka topic'

# Bắt đầu xử lý comment
if __name__ == "__main__":
    consumer_thread = Thread(target=process_comments)
    consumer_thread.start()
    
    app.run(debug=True)
