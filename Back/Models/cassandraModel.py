#!/usr/bin/env python3
import datetime
import logging
import random
import uuid

import time_uuid
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
        user_id TIMEUUID, 
        order_id TIMEUUID,
        product_id TIMEUUID,
        quantity DECIMAL,
        totaL_price DECIMAL,
        payment_method TEXT,
        date TIMESTAMP,
        PRIMARY KEY ((user_id),date)
    ) WITH CLUSTERING ORDER BY (date DESC)
"""

CREATE_TABLE_SEARCH_HISTORY = """
    CREATE TABLE IF NOT EXISTS search_history (
        user_id TIMEUUID, 
        search_query TEXT,
        time TIMESTAMP,
        product_id TIMEUUID,
        PRIMARY KEY ((user_id),time)
    ) WITH CLUSTERING ORDER BY (time DESC)
"""

CREATE_TABLE_PRODUCT_ANALYTICS = """
    CREATE TABLE IF NOT EXISTS product_analytics (
        product_id TIMEUUID, 
        total_orders INT,
        total_revenue INT,
        views INT,
        time TIMESTAMP,
        PRIMARY KEY ((product_id), total_orders)
    ) WITH CLUSTERING ORDER BY (total_orders DESC)
""" 

CREATE_TABLE_INVENTORY = """
    CREATE TABLE IF NOT EXISTS inventory (
        product_id TIMEUUID, 
        stock_level INT,
        last_updated TIMESTAMP,
        PRIMARY KEY ((product_id), stock_level)
    ) WITH CLUSTERING ORDER BY (stock_level DESC)
""" 
CREATE_TABLE_PROMOTIONS = """
    CREATE TABLE IF NOT EXISTS promotions (
        promo_code TIMEUUID, 
        discount_percentage INT,
        product_id TIMEUUID,
        start_date TIMESTAMP,
        end_date TIMESTAMP,
        PRIMARY KEY ((promo_code), discount_percentage)
    ) WITH CLUSTERING ORDER BY (discount_percentage DESC)
""" 
CREATE_TABLE_FEEDBACK = """
    CREATE TABLE IF NOT EXISTS feedback (
        user_id TIMEUUID, 
        feedback_text TEXT,
        time TIMESTAMP,
        status TEXT,
        PRIMARY KEY ((user_id), time)
    ) WITH CLUSTERING ORDER BY (time DESC)
""" 
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
    rows = session.execute(stmt, [user_id])
    return rows

#Q2. Query search history by product.
def get_search_history_by_product(session, user_id, product_id):
    stmt = session.prepare("SELECT * FROM search_history WHERE user_id = ? AND product_id = ?")
    rows = session.execute(stmt, [user_id, product_id])
    return rows

#PRODUCT ANALYTICS
#Q1. Query product analytics for a specific product.
def get_product_analytics(session, product_id):
    stmt = session.prepare("SELECT * FROM product_analytics WHERE product_id = ?")
    rows = session.execute(stmt, [product_id])
    return rows

#Q2. Query total revenue for all products
def get_total_revenue_for_all_products(session):
    stmt = session.prepare("SELECT product_id, total_revenue FROM product_analytics")
    rows = session.execute(stmt)
    return rows

#INVENTORY
#Q1. Query stock levels for a product.
def get_stock_level_by_product(session, product_id):
    stmt = session.prepare("SELECT stock_level, last_updated FROM inventory WHERE product_id = ?")
    rows = session.execute(stmt, [product_id])
    return rows

#Q2. Query products with stock below a threshold.
def get_products_with_low_stock(session, threshold):
    stmt = session.prepare("SELECT product_id, stock_level FROM inventory WHERE stock_level < ?")
    rows = session.execute(stmt, [threshold])
    return rows

#PROMOTIONS
#Q1. Query details of a promo code.
def get_promotion_details(session, promo_code):
    stmt = session.prepare("SELECT * FROM promotions WHERE promo_code = ?")
    rows = session.execute(stmt, [promo_code])
    return rows

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


