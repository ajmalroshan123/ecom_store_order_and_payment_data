import json
import time
import random
from kafka import KafkaProducer

topic_name = 'orders_data'

producer = KafkaProducer(bootstrap_servers = 'localhost:9092', value_serializer = lambda x : json.dumps(x).encode('utf-8'))

def generate_mock_data(order_id):
    items = ["Laptop", "Phone", "Book", "Tablet", "Monitor"]
    addresses = ["123 Main St, City A, Country", "456 Elm St, City B, Country", "789 Oak St, City C, Country"]
    statuses = ["Shipped", "Pending", "Delivered", "Cancelled"]

    return{
        'order_id': order_id,
        'customer_id':random.randint(100,1000),
        'item':random.choice(items),
        'quantity':random.randint(1,10),
        'price':random.uniform(100,500),
        'shipping_addresses': random.choice(addresses),
        'order_status': random.choice(statuses),
        'creation_data': '2024-03-30'
    }

order_id = 1
while True:
    data = generate_mock_data(order_id)

    try:
        producer.send(topic_name, value=data)
        print(f"Published Message with order ID : {order_id}")
    except Exception as e:
        print(f"Execution encountered : {e}") 

    time.sleep(2)

    order_id += 1
    if order_id >= 80:
        break       
