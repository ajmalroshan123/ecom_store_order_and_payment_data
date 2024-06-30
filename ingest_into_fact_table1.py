from kafka import KafkaConsumer, KafkaProducer
from cassandra.cluster import Cluster
import json

def kafka_consumer():
    return KafkaConsumer(
        'payments_data',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def kafka_producer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def cassandra_connection():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('ecom_store')  
    return cluster, session

def check_order_exists(session, order_id):
    query = "SELECT order_id FROM orders_payments_facts WHERE order_id = %s"
    rows = session.execute(query, [order_id])
    return len(list(rows)) > 0

def update_fact_table(session, payment_data):
    query = """
    UPDATE orders_payments_facts
    SET 
        card_last_four = %s,
        payment_datetime = %s,
        payment_id = %s,
        payment_method = %s,
        payment_status = %s
    WHERE order_id = %s
    """
    
    session.execute(query, (
        payment_data.get('card_last_four'),
        payment_data.get('payment_datetime'),
        int(payment_data.get('payment_id')) if payment_data.get('payment_id') else None,
        payment_data.get('payment_method'),
        payment_data.get('payment_status'),
        int(payment_data['order_id'])
    ))

def main():
    consumer = kafka_consumer()
    producer = kafka_producer()
    cluster, session = cassandra_connection()

    print("Starting to process messages...")
    for message in consumer:
        payment_data = message.value
        order_id = int(payment_data['order_id'])
        
        print(f"Received payment data for order_id: {order_id}")
        
        if check_order_exists(session, order_id):
            try:
                update_fact_table(session, payment_data)
                print(f"Updated order {order_id}")
            except Exception as e:
                print(f"Error updating order {order_id}: {str(e)}")
                producer.send('dlq_payments_data', value=payment_data)
                print(f"Sent order {order_id} to DLQ due to update error")
        else:
            producer.send('dlq', value=payment_data)
            print(f"Sent order {order_id} to DLQ (order not found in Cassandra)")

    cluster.shutdown()

if __name__ == "__main__":
    main()