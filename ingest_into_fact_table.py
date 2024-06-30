import json
from kafka import KafkaConsumer, KafkaProducer
from cassandra.cluster import Cluster
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Function to establish Cassandra connection
def cassandra_connection():
    try:
        # Configuration
        CASSANDRA_NODES = ['127.0.0.1']  # Cassandra node(s)
        CASSANDRA_PORT = 9042  # Default Cassandra port
        KEYSPACE = 'ecom_store'
        
        # Establish connection
        cluster = Cluster(contact_points=CASSANDRA_NODES, port=CASSANDRA_PORT)
        session = cluster.connect(KEYSPACE)

        logger.info("Cassandra connection established successfully.")
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to establish Cassandra connection: {e}")
        raise

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Kafka consumer configuration
consumer = KafkaConsumer('payments_data',
                         bootstrap_servers='localhost:9092',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Dead-letter queue (DLQ) topic
dlq_topic = 'dlq_payments_data'

# Function to process messages and update Cassandra
def process_messages(messages, session):
    try:
        for message in messages:
            # Extract JSON data
            json_data = message.value
            deserialized_data = json_data

            # Check if order_id exists in Cassandra
            query = f"SELECT order_id FROM orders_payments_facts WHERE order_id = {deserialized_data.get('order_id')} "
            rows = session.execute(query)

            if rows.one(): #if order_id found
                # Update data in Cassandra
                update_query = """
                               UPDATE orders_payment_facts
                               SET payment_id = %s,
                                   payment_method = %s, 
                                   card_last_four = %s, 
                                   payment_status = %s, 
                                   payment_datetime = %s 
                               WHERE order_id = %s
                               """
                values = (
                    deserialized_data.get('payment_id'),
                    deserialized_data.get('payment_method'),
                    deserialized_data.get('card_last_four'),
                    deserialized_data.get('payment_status'),
                    deserialized_data.get('payment_datetime'),
                    deserialized_data.get('order_id')
                ) 
                
                session.execute(update_query, values)
                logger.info("Data updated in Cassandra:", deserialized_data)
            else: # if order_id is not found
                producer.send(dlq_topic, value=json.dumps(deserialized_data))
                logger.info("Data sent to DLQ because order_id not found:", deserialized_data)
    except Exception as e:
        logger.error(f"Error processing messages: {e}")

# Function to pull messages in batches and process them
def pull_messages():
    try:
        # Setup Cassandra connection
        cluster, session = cassandra_connection()

        # Batch processing parameters
        batch_size = 100  # Adjust batch size as needed
        messages = []

        for message in consumer:
            messages.append(message)

            if len(messages) >= batch_size:
                process_messages(messages, session)
                messages = []
                
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        cluster.shutdown()

if __name__ == '__main__':
    pull_messages()
