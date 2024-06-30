from cassandra.cluster import Cluster

def cassandra_connection():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('ecom_store')
    return cluster, session

def check_order_exists(session, order_id):
    query = "SELECT order_id FROM orders_payments_facts WHERE order_id = %s"
    rows = session.execute(query, [order_id])
    return len(list(rows)) > 0

cluster, session = cassandra_connection()
order_id = 19  # Test with a known order_id
exists = check_order_exists(session, order_id)
print(f"Order {order_id} exists: {exists}")

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
    
    # Convert payment_id to bigint
    payment_id = int(payment_data.get('payment_id')) if payment_data.get('payment_id') else None
    
    # Keep payment_datetime as string
    payment_datetime = payment_data.get('payment_datetime')
    
    session.execute(query, (
        payment_data.get('card_last_four'),
        payment_datetime,  # Keep as string
        payment_id,
        payment_data.get('payment_method'),
        payment_data.get('payment_status'),
        int(payment_data['order_id'])
    ))

# Test data
test_data = {
    'order_id': 19,
    'card_last_four': '1234',
    'payment_datetime': '2024-03-30T01:01:30Z',
    'payment_id': '1001',
    'payment_method': 'Debit Card',
    'payment_status': 'Completed'
}

cluster, session = cassandra_connection()
update_fact_table(session, test_data)
print("Update completed")


def print_table_structure(session, table_name):
    query = f"SELECT column_name, type FROM system_schema.columns WHERE keyspace_name = 'ecom_store' AND table_name = '{table_name}'"
    rows = session.execute(query)
    for row in rows:
        print(f"{row.column_name}: {row.type}")

# Use it like this:
print_table_structure(session, 'orders_payments_facts')