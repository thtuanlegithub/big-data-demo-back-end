# File: kafka_consumer/consumer.py

from kafka import KafkaConsumer

consumer = KafkaConsumer('comments', 
                         bootstrap_servers='localhost:9092', 
                         group_id='comment_group',
                         auto_offset_reset='earliest',
                         enable_auto_commit=True)

for message in consumer:
    comment = message.value.decode('utf-8')
    print(comment)
    # Process comment with ML model
    
    # Gửi comment đến front-end
    requests.post('http://localhost:3000/comment', json={'comment': comment})
