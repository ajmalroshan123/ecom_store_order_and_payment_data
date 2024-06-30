import json
import time
import random
from kafka import KafkaProducer

topic_name = "payments_data"

producer = KafkaProducer(bootstrap_servers= 'localhost:9092', value_serializer=lambda x: json.dumps(x).encode('utf-8'))

payment_methods = ["Credit Card", "Debit Card", "PayPal", "Google Pay", "Apple Pay"]

def mock_data_payment(order_id):
    return{
        "payment_id":order_id + 1000,
        "order_id": order_id,
        "payment_method":random.choice(payment_methods),
        'card_last_four':str(order_id).zfill(4)[-4:],
        "payment_status": "Completed",
        "payment_datetime": f"2024-03-30T{str(order_id).zfill(2)}:01:30Z"
    }

for order_id in range(1,500):
    mock_payment = mock_data_payment(order_id)
    try:
        producer.send(topic_name, value=mock_payment)
        print(f"Published Message with order ID : {order_id}")
    except Exception as e:
        print(f'Execution encountered:{e}') 
           