import json
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
#from cassandra.auth import PlainTextAuthProvider


def cassandra_connection():
    CASSANDRA_NODES = ['127.0.0.1']
    CASSANDRA_PORT = 9042
    KEYSPACE = 'ecom_store'

    # Connection setup (without authentication)
    cluster = Cluster(contact_points=CASSANDRA_NODES, port=CASSANDRA_PORT)
    session = cluster.connect(KEYSPACE)

    return cluster, session


cluster, session = cassandra_connection()

insert_stmt = session.prepare("""
    INSERT INTO orders_payments_facts (order_id, customer_id, item, quantity, price, shipping_address, order_status, creation_date, payment_id, payment_method, card_last_four, payment_status, payment_datetime)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
consumer = KafkaConsumer("orders_data",
                        bootstrap_servers = 'localhost:9092',
                        value_deserializer = lambda x: json.loads(x.decode('utf-8')))

def pull_messages():
    for message in consumer:
        json_data = message.value
        #deserialized the json data
        deserialized_data = json_data

        print(deserialized_data)

        cassandra_data = (
            deserialized_data.get("order_id"),
            deserialized_data.get("customer_id"),
            deserialized_data.get("item"),
            deserialized_data.get("quantity"),
            deserialized_data.get("price"),
            deserialized_data.get("shipping_address"),
            deserialized_data.get("order_status"),
            deserialized_data.get("creation_date"),
            None,
            None,
            None,
            None,
            None
        )

        session.execute(insert_stmt, cassandra_data)

        print("Data inserted in Cassandra !!")
if __name__ == "__main__":
    try:
        cluster, session = cassandra_connection()
        pull_messages()
    except KeyboardInterrupt:
        pass
    finally:
        cluster.shutdown()
    