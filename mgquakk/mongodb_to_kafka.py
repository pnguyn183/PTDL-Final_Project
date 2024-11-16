import json
from confluent_kafka import Producer
from pymongo import MongoClient

# Thiết lập kết nối tới MongoDB
def get_mongo_data():
    try:
        client = MongoClient('mongodb://mymongodb:27017/')  
        db = client['dbcardata'] 
        collection = db['cars']
        
        data = list(collection.find({}, {'_id': 0}))
        print(f"Đã kết nối MongoDB thành công. Tìm thấy {len(data)} records")
        return data
    except Exception as e:
        print(f"Lỗi kết nối MongoDB: {e}")
        return []

# Thiết lập Kafka Producer)
def send_data_to_kafka():
    try:
        producer = Producer({
            'bootstrap.servers': 'kafka_container',
            'client.id': 'car_producer'
        })
        
        data = get_mongo_data()
        if not data:
            print("Không có dữ liệu để xử lý")
            return
            
        for record in data:
            # Encode với ensure_ascii=False để giữ nguyên ký tự Unicode
            value = json.dumps(record, ensure_ascii=False)
            producer.produce('cars', 
                           value=value.encode('utf-8'),
                           callback=lambda err, msg: print(f'Đã gửi: {msg.value().decode("utf-8")}') if err is None else print(f'Lỗi: {err}')
            )
            producer.poll(0)
            
        producer.flush()
        print("Hoàn thành việc gửi dữ liệu vào Kafka")
        
    except Exception as e:
        print(f"Lỗi trong quá trình xử lý Kafka: {e}")

# Chạy hàm gửi dữ liệu
if __name__ == '__main__':
    send_data_to_kafka()
    print("Đã gửi toàn bộ dữ liệu từ MongoDB vào Kafka.")