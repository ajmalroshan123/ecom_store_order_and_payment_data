from cassandra.cluster import Cluster

def cassandra_connection():
    try:
        CASSANDRA_NODES = ['127.0.0.1']  # Cassandra node(s)
        CASSANDRA_PORT = 9042  # Default Cassandra port
        KEYSPACE = 'ecom_store'  # Replace with your actual keyspace name
        
        # Establish connection
        cluster = Cluster(contact_points=CASSANDRA_NODES, port=CASSANDRA_PORT)
        session = cluster.connect(KEYSPACE)
        return cluster, session
    except Exception as e:
        print(f"Can't Connect: {str(e)}")
        return None, None
    
def fetch_all_order_ids(session):
    query = "SELECT order_id FROM orders_payments_facts"
    
    try:
        rows = session.execute(query)
        
        print("All Order IDs:")
        for row in rows:
            print(row.order_id)
        
        return [row.order_id for row in rows]  # Return list of order_ids if needed
    except Exception as e:
        print(f"Error executing query: {str(e)}")
        return []

# Main execution
cluster, session = cassandra_connection()

if session:
    fetch_all_order_ids(session)
    
    # Close the connection
    cluster.shutdown()
else:
    print("Failed to establish connection to Cassandra.")