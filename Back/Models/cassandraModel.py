#!/usr/bin/env python3
import datetime
import logging
import random
import uuid

# Set logger
log = logging.getLogger()

#import time_uuid
from cassandra.query import BatchStatement
from cassandra.cluster import Cluster

def init_cassandra(cassandra_uri, keyspace):
    cluster = Cluster([cassandra_uri])
    session = cluster.connect()
    session.set_keyspace(keyspace)
    return session


CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

CREATE_TABLE_PURCHASE_ORDERS = """
    CREATE TABLE IF NOT EXISTS purchase_order (
        user_id TEXT,  -- Cambiado de INT a TEXT
        order_id TEXT,  -- Cambiado de INT a TEXT
        product_id TEXT,  -- Cambiado de INT a TEXT
        quantity DECIMAL,
        total_price DECIMAL,
        payment_method TEXT,
        date TIMESTAMP,
        PRIMARY KEY ((user_id), date)
    ) WITH CLUSTERING ORDER BY (date DESC)
"""

CREATE_TABLE_SEARCH_HISTORY = """
    CREATE TABLE IF NOT EXISTS search_history (
        user_id TEXT,  -- Cambiado de INT a TEXT
        search_query TEXT,
        time TIMESTAMP,
        product_id TEXT,  -- Cambiado de INT a TEXT
        PRIMARY KEY ((user_id), time)
    ) WITH CLUSTERING ORDER BY (time DESC)
"""

CREATE_TABLE_PRODUCT_ANALYTICS = """
    CREATE TABLE IF NOT EXISTS product_analytics (
        product_id TEXT,  -- Cambiado de INT a TEXT
        total_orders INT,
        total_revenue INT,
        views INT,
        time TIMESTAMP,
        PRIMARY KEY ((product_id), total_orders)
    ) WITH CLUSTERING ORDER BY (total_orders DESC)
""" 

CREATE_TABLE_INVENTORY = """
    CREATE TABLE IF NOT EXISTS inventory (
        product_id TEXT,  -- Cambiado de INT a TEXT
        stock_level INT,
        last_updated TIMESTAMP,
        PRIMARY KEY ((product_id), stock_level)
    ) WITH CLUSTERING ORDER BY (stock_level DESC)
""" 

CREATE_TABLE_PROMOTIONS = """
    CREATE TABLE IF NOT EXISTS promotions (
        promo_code TEXT,  -- Cambiado de INT a TEXT
        discount_percentage INT,
        product_id TEXT,  -- Cambiado de INT a TEXT
        start_date TIMESTAMP,
        end_date TIMESTAMP,
        PRIMARY KEY ((promo_code), discount_percentage)
    ) WITH CLUSTERING ORDER BY (discount_percentage DESC)
""" 

CREATE_TABLE_FEEDBACK = """
    CREATE TABLE IF NOT EXISTS feedback (
        user_id TEXT,  -- Cambiado de INT a TEXT
        feedback_text TEXT,
        time TIMESTAMP,
        status TEXT,
        PRIMARY KEY ((user_id), time)
    ) WITH CLUSTERING ORDER BY (time DESC)
""" 



import uuid
import random
import datetime

import random
import datetime
from cassandra.query import BatchStatement

def execute_batch(session, stmt, data):
    batch_size = 10
    for i in range(0, len(data), batch_size):
        batch = BatchStatement()
        for item in data[i : i + batch_size]:
            batch.add(stmt, item)
        session.execute(batch)
    session.execute(batch)

def bulk_insert(session):
    # Preparar las sentencias para cada tabla
    po_stmt = session.prepare("""
        INSERT INTO purchase_order (user_id, order_id, product_id, quantity, total_price, payment_method, date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)
    
    search_stmt = session.prepare("""
        INSERT INTO search_history (user_id, search_query, time, product_id)
        VALUES (?, ?, ?, ?)
    """)
    
    analytics_stmt = session.prepare("""
        INSERT INTO product_analytics (product_id, total_orders, total_revenue, views, time)
        VALUES (?, ?, ?, ?, ?)
    """)
    
    inventory_stmt = session.prepare("""
        INSERT INTO inventory (product_id, stock_level, last_updated)
        VALUES (?, ?, ?)
    """)
    
    promo_stmt = session.prepare("""
        INSERT INTO promotions (promo_code, discount_percentage, product_id, start_date, end_date)
        VALUES (?, ?, ?, ?, ?)
    """)
    
    feedback_stmt = session.prepare("""
        INSERT INTO feedback (user_id, feedback_text, time, status)
        VALUES (?, ?, ?, ?)
    """)

    # Generación de datos
    users = [str(i) for i in range(1, 9)]  # Usar strings para los user_id
    products = [str(i) for i in range(1, 6)]  # Usar strings para los product_id
    payment_methods = ["Credit Card", "Paypal", "Bank Transfer", "Gift Card"]
    search_queries = ["laptop", "smartphone", "headphones", "watch", "shoes"]
    feedback_texts = ["Great product!", "Not bad", "Poor quality", "Excellent!", "Average"]
    statuses = ["positive", "negative", "neutral"]

    # Llamada a BatchStatement para insertar datos
    batch = BatchStatement()

    # Insertar datos en la tabla purchase_order
    for _ in range(8):
        user_id = random.choice(users)
        order_id = str(random.randint(1000, 9999))  # Convertido a string
        product_id = random.choice(products)
        quantity = random.randint(1, 10)
        total_price = random.uniform(10.0, 500.0)
        payment_method = random.choice(payment_methods)
        date = datetime.datetime.now()
        batch.add(po_stmt, (user_id, order_id, product_id, quantity, total_price, payment_method, date))

    # Insertar datos en la tabla search_history
    for i in range(8):
        user_id = users[i]  # Usar user_id como string
        search_query = random.choice(search_queries)
        time = datetime.datetime.now()
        product_id = random.choice(products)  # Usar product_id como string
        batch.add(search_stmt, (user_id, search_query, time, product_id))

    # Insertar datos en la tabla product_analytics
    for _ in range(5):
        product_id = random.choice(products)
        total_orders = random.randint(10, 100)
        total_revenue = random.randint(10, 100)
        views = random.randint(50, 500)
        time = datetime.datetime.now()
        batch.add(analytics_stmt, (product_id, total_orders, total_revenue, views, time))

    # Insertar datos en la tabla inventory
    for _ in range(5):
        product_id = random.choice(products)
        stock_level = random.randint(0, 100)
        last_updated = datetime.datetime.now()
        batch.add(inventory_stmt, (product_id, stock_level, last_updated))

    # Insertar datos en la tabla promotions
    for _ in range(5):
        promo_code = str(random.randint(1000, 9999))  # Convertido a string
        product_id = random.choice(products)
        discount_percentage = random.randint(5, 50)
        start_date = datetime.datetime.now()
        end_date = start_date + datetime.timedelta(days=random.randint(5, 30))
        batch.add(promo_stmt, (promo_code, discount_percentage, product_id, start_date, end_date))

    # Insertar datos en la tabla feedback
    for _ in range(5):
        user_id = random.choice(users)
        feedback_text = random.choice(feedback_texts)
        time = datetime.datetime.now()
        status = random.choice(statuses)
        batch.add(feedback_stmt, (user_id, feedback_text, time, status))

    # Ejecutar el batch statement
    session.execute(batch)
    print("Data Inserted into all tables")



def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_TABLE_PURCHASE_ORDERS)
    session.execute(CREATE_TABLE_SEARCH_HISTORY)
    session.execute(CREATE_TABLE_PRODUCT_ANALYTICS)
    session.execute(CREATE_TABLE_INVENTORY)
    session.execute(CREATE_TABLE_PROMOTIONS)
    session.execute(CREATE_TABLE_FEEDBACK)
    print("Schema creation complete")

def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))

def erase_all_data(session):
    # List of tables to delete data from
    session.execute("DROP TABLE IF EXISTS purchase_order")
    session.execute("DROP TABLE IF EXISTS search_history")
    session.execute("DROP TABLE IF EXISTS product_analytics")
    session.execute("DROP TABLE IF EXISTS inventory")
    session.execute("DROP TABLE IF EXISTS promotions")
    session.execute("DROP TABLE IF EXISTS feedback")

    print("Tables deleted successfully.")

#PURCHASE ORDER
#Q1. Query all orders by a user.
def get_all_orders_by_user(session, user_id):
    stmt = session.prepare("SELECT * FROM purchase_order WHERE user_id = ?")
    rows = session.execute(stmt, [user_id])
    return rows

#Q2. Query total price and payment method for a user's orders within a date range.
def get_orders_by_date_range(session, user_id, start_date, end_date):
    stmt = session.prepare("SELECT total_price, payment_method FROM purchase_order WHERE user_id = ? AND date >= ? AND date <= ?")
    rows = session.execute(stmt, [user_id, start_date, end_date])
    return rows

#Q3. Query orders by product within a date range.
def get_orders_by_product_and_date(session, user_id, product_id, start_date, end_date):
    stmt = session.prepare("SELECT * FROM purchase_order WHERE user_id = ? AND product_id = ? AND date >= ? AND date <= ?")
    rows = session.execute(stmt, [user_id, product_id, start_date, end_date])
    return rows

#SEARCH HISTORY
#Q1. Query all search history for a user.
def get_all_search_history_by_user(session, user_id):
    stmt = session.prepare("SELECT * FROM search_history WHERE user_id = ?")
    # Convert user_id to string (TEXT)
    rows = session.execute(stmt, [str(user_id)])
    return rows
#Q2. Query search history by product.
def get_search_history_by_product(session, user_id, product_id):
    stmt = session.prepare("SELECT * FROM search_history WHERE user_id = ? AND product_id = ?")
    rows = session.execute(stmt, [user_id, product_id])
    return rows

#PRODUCT ANALYTICS
#Q1. Query product analytics for a specific product.
def get_product_analytics(session, product_id):
    try:
        stmt = session.prepare("SELECT * FROM product_analytics WHERE product_id = ?")
        rows = session.execute(stmt, [product_id])

        if not rows:
            print(f"No analytics found for product ID: {product_id}")
            return

        # Print a header
        print(f"\n--- Product Analytics for Product ID: {product_id} ---")

        for row in rows:
            print(f"Product ID      : {row.product_id}")
            print(f"Views           : {row.views}")
            print(f"Purchases       : {row.purchases}")
            print(f"Ratings Average : {row.avg_rating:.2f}")
            print(f"Last Updated    : {row.last_updated}")
            print("-" * 40)
    except Exception as e:
        print(f"An error occurred while fetching product analytics: {e}")

#Q2. Query total revenue for all products
def get_total_revenue_for_all_products(session):
    stmt = session.prepare("SELECT product_id, total_revenue FROM product_analytics")
    rows = session.execute(stmt)
    return rows

#INVENTORY
#Q1. Query stock levels for a product.
def get_stock_level_by_product(session, product_id):
    try:
        stmt = session.prepare("SELECT stock_level, last_updated FROM inventory WHERE product_id = ?")
        rows = session.execute(stmt, [product_id])

        if not rows:
            print(f"No stock information found for product ID: {product_id}")
            return

        # Print a header
        print("\n--- Stock Level Details ---")
        for row in rows:
            print(f"Product ID     : {product_id}")
            print(f"Stock Level    : {row.stock_level}")
            print(f"Last Updated   : {row.last_updated}")
            print("-" * 30)
    except Exception as e:
        print(f"An error occurred while fetching stock levels: {e}")
#Q2. Query products with stock below a threshold.
def get_products_with_low_stock(session, threshold):
    stmt = session.prepare("SELECT product_id, stock_level FROM inventory WHERE stock_level < ?")
    rows = session.execute(stmt, [threshold])
    return rows

#PROMOTIONS
#Q1. Query details of a promo code.
def get_promotion_details(session, promo_code):
    try:
        stmt = session.prepare("SELECT * FROM promotions WHERE promo_code = ?")
        rows = session.execute(stmt, [promo_code])
        
        if not rows:
            print(f"No details found for promo code: {promo_code}")
            return

        # Print a header
        print("\n--- Promotion Details ---")
        for row in rows:
            print(f"Promo Code       : {row.promo_code}")
            print(f"Discount         : {row.discount_percentage}%")
            print(f"Product ID       : {row.product_id}")
            print(f"Start Date       : {row.start_date}")
            print(f"End Date         : {row.end_date or 'No end date specified'}")
            print("-" * 30)
    except Exception as e:
        print(f"An error occurred while fetching promo details: {e}")

#Q2. Query active promotions for a product within a date range.
def get_active_promotions(session, product_id, start_date, end_date):
    stmt = session.prepare("""
        SELECT * FROM promotions WHERE product_id = ? AND start_date <= ? AND end_date >= ?
    """)
    rows = session.execute(stmt, [product_id, end_date, start_date])
    return rows

#FEEDBACK
#Q1. Query all feedback for a user.
def get_feedback_by_user(session, user_id):
    stmt = session.prepare("SELECT * FROM feedback WHERE user_id = ?")
    rows = session.execute(stmt, [user_id])
    return rows

#Q2. Query feedback submitted within a date range.
def get_feedback_by_date_range(session, start_date, end_date):
    stmt = session.prepare("SELECT * FROM feedback WHERE time >= ? AND time <= ?")
    rows = session.execute(stmt, [start_date, end_date])
    return rows

#ABANDONED CARTS
#Q1. Query abandoned carts by user.
def get_abandoned_carts_by_user(session, user_id):
    stmt = session.prepare("SELECT * FROM abandoned_carts WHERE user_id = ?")
    rows = session.execute(stmt, [user_id])
    return rows

#Q2. Query abandoned carts within a specific timeframe.
def get_abandoned_carts_by_timeframe(session, start_date, end_date):
    stmt = session.prepare("SELECT * FROM abandoned_carts WHERE time >= ? AND time <= ?")
    rows = session.execute(stmt, [start_date, end_date])
    return rows



#ADDING
#Add to search_products
import random
import datetime
from cassandra.query import BatchStatement
def insert_search_history(session, user_id, search_query, product_id):
    search_stmt = session.prepare("""
        INSERT INTO search_history (user_id, search_query, time, product_id)
        VALUES (?, ?, ?, ?)
    """)
    batch = BatchStatement()
    time = datetime.datetime.now()
    batch.add(search_stmt, (user_id, search_query, time, product_id))
    session.execute(batch)

def insert_purchase_order(session, user_id, order_id, product_id, quantity, total_price, payment_method):
    po_stmt = session.prepare("""
        INSERT INTO purchase_order (user_id, order_id, product_id, quantity, total_price, payment_method, date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)
    batch = BatchStatement()
    date = datetime.datetime.now()
    batch.add(po_stmt, (user_id, order_id, product_id, quantity, total_price, payment_method, date))
    session.execute(batch)

def insert_product_analytics(session, product_id, total_orders, total_revenue, views):
    analytics_stmt = session.prepare("""
        INSERT INTO product_analytics (product_id, total_orders, total_revenue, views, time)
        VALUES (?, ?, ?, ?, ?)
    """)
    batch = BatchStatement()
    time = datetime.datetime.now()
    batch.add(analytics_stmt, (product_id, total_orders, total_revenue, views, time))
    session.execute(batch)

def insert_inventory(session, product_id, stock_level):
    inventory_stmt = session.prepare("""
        INSERT INTO inventory (product_id, stock_level, last_updated)
        VALUES (?, ?, ?)
    """)
    batch = BatchStatement()
    last_updated = datetime.datetime.now()
    batch.add(inventory_stmt, (product_id, stock_level, last_updated))
    session.execute(batch)

def insert_promotions(session, promo_code, discount_percentage, product_id, start_date, end_date):
    promo_stmt = session.prepare("""
        INSERT INTO promotions (promo_code, discount_percentage, product_id, start_date, end_date)
        VALUES (?, ?, ?, ?, ?)
    """)
    batch = BatchStatement()
    batch.add(promo_stmt, (promo_code, discount_percentage, product_id, start_date, end_date))
    session.execute(batch)

def insert_feedback(session, user_id, feedback_text, status):
    feedback_stmt = session.prepare("""
        INSERT INTO feedback (user_id, feedback_text, time, status)
        VALUES (?, ?, ?, ?)
    """)
    batch = BatchStatement()
    time = datetime.datetime.now()
    batch.add(feedback_stmt, (user_id, feedback_text, time, status))
    session.execute(batch)
