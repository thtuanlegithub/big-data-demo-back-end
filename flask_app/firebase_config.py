import firebase_admin
from firebase_admin import credentials

# Đường dẫn đến tệp firebase-adminsdk.json
cred = credentials.Certificate("./firebase-adminsdk.json")

# Khởi tạo Firebase
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://classify-comments-default-rtdb.asia-southeast1.firebasedatabase.app'
})

# Firebase database reference
comments_ref = firebase_admin.db.reference('comments')
