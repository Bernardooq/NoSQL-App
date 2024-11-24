from pymongo import MongoClient
from datetime import datetime

# Mongo connection
def init_mongo(mongo_uri, db_name):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    return client, db

# Insert User
def add_mongo_user(mongodb_database, user_id, name, email, hashed_password):
    try:
        user = {
            "user_id": user_id,
            "name": name,
            "email": email,
            "hashed_password": hashed_password,
            "registration_date": datetime.utcnow()  # Fecha de registro
        }
        mongodb_database.users.insert_one(user)
    except Exception as e:
        print(f"Error inserting user into MongoDB: {e}")
        raise

# Get User
def get_mongo_user(mongodb_database, user_id):
    try:
        user = mongodb_database.users.find_one({"user_id": user_id})
        return user
    except Exception as e:
        print(f"Error getting user from MongoDB: {e}")
        raise 

# Purschase
def add_purchase_order(mongodb_database, order_id, user_id, products, total_price, payment_method, order_status="Pending"):
    try:
        order = {
            "order_id": order_id,
            "user_id": user_id,
            "products": products,  # Lista de diccionarios con productID, quantity, price
            "total_price": total_price,
            "payment_method": payment_method,
            "order_status": order_status,
            "order_date": datetime.utcnow()
        }
        mongodb_database.orders.insert_one(order)
    except Exception as e:
        print(f"Error inserting purchase order into MongoDB: {e}")
        raise

# User history
def get_user_order_history(mongodb_database, user_id):
    try:
        orders = mongodb_database.orders.find({"user_id": user_id}).sort("order_date", -1)  # Ordenar por fecha descendente
        return list(orders)
    except Exception as e:
        print(f"Error getting order history from MongoDB: {e}")
        raise

# Product catalog
def add_product(mongodb_database, product_id, name, description, price, stock, category, image_url):
    try:
        product = {
            "product_id": product_id,
            "name": name,
            "description": description,
            "price": price,
            "stock": stock,
            "category": category,
            "image_url": image_url
        }
        mongodb_database.products.insert_one(product)
    except Exception as e:
        print(f"Error inserting product into MongoDB: {e}")
        raise

# Search Products
def search_products(mongodb_database, query):
    try:
        products = mongodb_database.products.find({
            "$text": {"$search": query}
        })
        return list(products)
    except Exception as e:
        print(f"Error searching products in MongoDB: {e}")
        raise

# Add to shopping cart
def add_to_cart(mongodb_database, user_id, product_id, product_name, quantity, price, description):
    try:
        cart_item = {
            "user_id": user_id,
            "product_id": product_id,
            "product_name": product_name,
            "quantity": quantity,
            "price": price,
            "description": description
        }
        mongodb_database.carts.insert_one(cart_item)
    except Exception as e:
        print(f"Error adding product to cart in MongoDB: {e}")
        raise

# Get User shopping cart
def get_user_cart(mongodb_database, user_id):
    try:
        cart_items = mongodb_database.carts.find({"user_id": user_id})
        return list(cart_items)
    except Exception as e:
        print(f"Error getting user cart from MongoDB: {e}")
        raise

# Insert product at wishlist
def add_to_wishlist(mongodb_database, user_id, product_id):
    try:
        wishlist_item = {
            "user_id": user_id,
            "product_id": product_id,
            "date_added": datetime.utcnow()
        }
        mongodb_database.wishlist.insert_one(wishlist_item)
    except Exception as e:
        print(f"Error adding product to wishlist in MongoDB: {e}")
        raise

# Get user wishlist
def get_user_wishlist(mongodb_database, user_id):
    try:
        wishlist_items = mongodb_database.wishlist.find({"user_id": user_id})
        return list(wishlist_items)
    except Exception as e:
        print(f"Error getting user wishlist from MongoDB: {e}")
        raise

# Update user info
def update_user_info(mongodb_database, user_id, updated_info):
    try:
        mongodb_database.users.update_one(
            {"user_id": user_id},
            {"$set": updated_info}
        )
    except Exception as e:
        print(f"Error updating user info in MongoDB: {e}")
        raise

# Product feedback
def add_product_feedback(mongodb_database, user_id, product_id, feedback_text):
    try:
        feedback = {
            "user_id": user_id,
            "product_id": product_id,
            "feedback_text": feedback_text,
            "timestamp": datetime.utcnow()
        }
        mongodb_database.product_feedback.insert_one(feedback)
    except Exception as e:
        print(f"Error inserting product feedback in MongoDB: {e}")
        raise

# Return request
def add_return_request(mongodb_database, order_id, product_id, reason):
    try:
        return_request = {
            "order_id": order_id,
            "product_id": product_id,
            "reason": reason,
            "status": "Pending",
            "request_date": datetime.utcnow()
        }
        mongodb_database.return_requests.insert_one(return_request)
    except Exception as e:
        print(f"Error inserting return request into MongoDB: {e}")
        raise

# Order status
def get_order_status(mongodb_database, order_id):
    try:
        order = mongodb_database.orders.find_one({"order_id": order_id})
        return order["order_status"]
    except Exception as e:
        print(f"Error getting order status from MongoDB: {e}")
        raise
