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

CREATE_TABLE_ABANDONED_CART = """
    CREATE TABLE IF NOT EXISTS abandoned_cart (
        user_id TEXT,  
        items LIST<TEXT>,
        time TIMESTAMP,
        value DECIMAL,
        PRIMARY KEY ((user_id), time)
    ) WITH CLUSTERING ORDER BY (time ASC)

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

    abandoned_stmt = session.prepare("""
        INSERT INTO abandoned_cart (user_id, items, time, value)
        VALUES (?, ?, ?, ? )
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
    session.execute(CREATE_TABLE_ABANDONED_CART)
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
    session.execute("DROP TABLE IF EXISTS abandoned_cart")

    print("Tables deleted successfully.")

#PURCHASE ORDER
#Q1. Query all orders by a user.
def retrieve_user_orders(session, user_id):
    try:
        # Prepare the query to fetch all orders for the given user_id
        stmt = session.prepare("SELECT * FROM purchase_order WHERE user_id = ?")
        rows = session.execute(stmt, [user_id])

        # Check if any orders are found
        if not rows:
            print(f"No purchase orders found for user ID: {user_id}")
            return

        # Iterate through the rows and print each order in a formatted manner
        print(f"Purchase Orders for User ID: {user_id}")
        print("=" * 40)
        for row in rows:
            print(f"Order ID          : {row.order_id}")
            print(f"Product ID        : {row.product_id}")
            print(f"Quantity          : {row.quantity}")
            print(f"Total Price       : {row.total_price}")
            print(f"Payment Method    : {row.payment_method}")
            print(f"Date              : {row.date}")
            print("-" * 40)
    except Exception as e:
        print(f"An error occurred while fetching purchase orders: {e}")

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
            print(f"Product ID         : {row.product_id}")
            print(f"Views              : {row.views}")
            print(f"Total_orders       : {row.total_orders}")
            print(f"Total_revenue      : {row.total_revenue}")
            print(f"Up since           : {row.time}")
            print("-" * 40)
    except Exception as e:
        print(f"An error occurred while fetching product analytics: {e}")

#Q2. Query total revenue for all products
def get_total_revenue_for_all_products(session):
    stmt = session.prepare("SELECT product_id, total_revenue FROM product_analytics")
    rows = session.execute(stmt)
    return rows


def increase_views(session, product_ids):
    # Prepare statements for selecting and updating
    select_stmt = session.prepare("SELECT total_orders FROM product_analytics WHERE product_id = ?")
    update_stmt = session.prepare("UPDATE product_analytics SET views = ? WHERE product_id = ? AND total_orders = ?")
    
    results = {}  # Dictionary to store results for each product_id

    for product_id in product_ids:
        # Retrieve total_orders for the current product_id
        rows = session.execute(select_stmt, [product_id])
        
        # Process the results
        found = False
        for row in rows:
            total_orders = row.total_orders
            found = True
            #print(f"Product ID: {product_id}, Total Orders: {total_orders}")
            
            # Retrieve current views
            views_stmt = session.prepare("SELECT * FROM product_analytics WHERE product_id = ? AND total_orders = ?")
            view_rows = session.execute(views_stmt, [product_id, total_orders])

            # Iterate and print each row as a dictionary
            for row in view_rows:
                new_views = int(row.views) + 1
                session.execute(update_stmt, [new_views, product_id, total_orders])
                
            
            break  #
        
        if not found:
            print(f"No records found for product_id: {product_id}")
            results[product_id] = None  # Indicate no results for this product_id
    
    return results  # Return all results as a dictionary
def increase_orders_and_revenue(session, product_id, order_quantity, price_per_unit):
    """
    Increases the total_orders and total_revenue for a given product by the new order quantity.
    Deletes the old row and inserts a new row with the updated total_orders and total_revenue.
    """
    # Prepare the necessary statements for selecting, deleting, and inserting
    select_stmt = session.prepare("SELECT * FROM product_analytics WHERE product_id = ? AND total_orders = ?")
    delete_stmt = session.prepare("DELETE FROM product_analytics WHERE product_id = ? AND total_orders = ?")
    insert_stmt = session.prepare("""
        INSERT INTO product_analytics (product_id, total_orders, views, total_revenue, time) 
        VALUES (?, ?, ?, ?, ?)
    """)

    # Fetch the current total_orders and revenue for the given product_id
    fetch_stmt = session.prepare("SELECT total_orders, total_revenue FROM product_analytics WHERE product_id = ?")
    rows = session.execute(fetch_stmt, [product_id])

    for row in rows:
        total_orders = row.total_orders
        current_revenue = row.total_revenue
        #print(f"Product ID: {product_id}, Total Orders (before update): {total_orders}, Total Revenue (before update): {current_revenue}")

        # Calculate the additional revenue from the new order
        additional_revenue = order_quantity * price_per_unit
        new_revenue = current_revenue + additional_revenue
        new_total_orders = total_orders + order_quantity  # Increase total_orders by the new order quantity

        # Fetch detailed row information for the specific product_id and total_orders
        detail_rows = session.execute(select_stmt, [product_id, total_orders])

        for detail_row in detail_rows:
            # print("Current Row Details:")
            # print(detail_row._asdict())  # Print all attributes of the current row

            # Delete the old row with the old total_orders
            session.execute(delete_stmt, [product_id, total_orders])
            #print(f"Deleted row for Product ID: {product_id}, Total Orders: {total_orders}")

            # Insert the updated row with the new total_orders and total_revenue
            session.execute(insert_stmt, [
                product_id,
                new_total_orders,
                detail_row.views,
                new_revenue,  # Set the updated revenue
                detail_row.time
            ])
            #print(f"Inserted new row for Product ID: {product_id}, Total Orders: {new_total_orders}, Total Revenue: {new_revenue}")
            return new_total_orders, new_revenue  # Return the updated values

    # If no row was found for the product_id
    print(f"No records found for product_id: {product_id}")
    return None, None  # Indicate no results for this product_id




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
    stmt = session.prepare("SELECT * FROM abandoned_cart WHERE user_id = ?")
    rows = session.execute(stmt, [user_id])
    
    abandoned_list = []
    for row in rows:
        cart = {
            "items": row.items,
            "value": row.value,
            "time": row.time
        }
    abandoned_list.append(cart)
    
    return abandoned_list

#Q2. Query abandoned carts within a specific timeframe.
def get_abandoned_carts_by_timeframe(session, start_date, end_date):
    stmt = session.prepare("SELECT * FROM abandoned_cart WHERE time >= ? AND time <= ?")
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


def insert_abandoned_cart(session, user_id, items, value):
    try:
        # Prepare the statement
        abandoned_stmt = session.prepare("""
            INSERT INTO abandoned_cart (user_id, items, time, value)
            VALUES (?, ?, ?, ?)
        """)
        
        # Current timestamp
        current_time = datetime.datetime.now()

        # Execute the prepared statement
        session.execute(
            abandoned_stmt,
            [user_id, items, current_time, value]
        )

        print(f"Abandoned cart added for user_id {user_id} with {len(items)} items at {current_time}")
    except Exception as e:
        print(f"An error occurred while inserting into abandoned_cart: {e}")
