from pymongo import MongoClient
from datetime import datetime

# Mongo connection
def init_mongo(mongo_uri, db_name):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    return client, db

# Insert User
def add_mongo_user(mongodb_database, username, email, hashed_password):
    try:
        user = {
            "username": username,
            "email": email,
            "hashed_password": hashed_password,
            "registration_date": datetime.now()  # Fecha de registro
        }
        mongodb_database.users.insert_one(user)
    except Exception as e:
        print(f"Error inserting user into MongoDB: {e}")
        raise

# Get User
def get_mongo_user(mongodb_database, email):
    try:
        user = mongodb_database.users.find_one({"email": email})
        return user
    except Exception as e:
        print(f"Error getting user from MongoDB: {e}")
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

def find_product(mongodb_database, product_id):
    try:
        product=mongodb_database.products.find_one({"product_id": product_id})
        return product
    except Exception as e:
        print(f"Error inserting product into MongoDB: {e}")

def update_product(mongodb_database, product_id, name, description, price, stock, category, image_url):
    try:
        product= mongodb_database.products.update_one({"product_id": product_id,}, 
            {"$set": {"name": name, "description": description, "price": price, "stock": stock, "category" : category, "image_url": image_url}})
        return product
    except Exception as e:
        print("Error updating product")
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

def find_product(mongodb_database, product_id):
    try:
        product= mongodb_database.products.find_one({"product_id": product_id})
        return(product)
    except Exception as e:
        print(f"Error finding product in MongoDB: {e}")

# Add to shopping cart
def add_to_cart(mongodb_database, email, product_id, product_name, quantity, price, description):
    try:
        cart_item = {
            "email": email, 
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
def get_user_cart(mongodb_database, email):
    try:
        cart_items = mongodb_database.carts.find({"email": email})
        return list(cart_items)
    except Exception as e:
        print(f"Error getting user cart from MongoDB: {e}")
        raise

def edit_user_cart(mongodb_database, email, new_cart):
    try:
         if new_cart["quantity"]==0:
             mongodb_database.carts.delete_one({"email": email}, {"product_id": new_cart["product_id"]})
        
        
         mongodb_database.carts.update_one(
                {"email": email, "product_id": new_cart["product_id"]},  # Filtrar por email y product_id
                {"$set": {
                    "quantity": new_cart["quantity"],
                }},
                upsert=True  # Si el producto no existe, se inserta como nuevo
            )
    except Exception as e:
        print(f"Error updating user cart from MongoDB: {e}")

# Insert product at wishlist
def add_to_wishlist(mongodb_database, email, product_id):
    try:
        wishlist_item = {
            "email": email,
            "product_id": product_id,
            "date_added": datetime.now()
        }
        mongodb_database.wishlist.insert_one(wishlist_item)
    except Exception as e:
        print(f"Error adding product to wishlist in MongoDB: {e}")
        raise

# Get user wishlist
def get_user_wishlist(mongodb_database, email):
    try:
        wishlist_items = mongodb_database.wishlist.find({"email": email})
        return list(wishlist_items)
    except Exception as e:
        print(f"Error getting user wishlist from MongoDB: {e}")
        raise

def edit_user_wishlist(mongodb_database, email, new_wishlist):
    try:
        wishlist= mongodb_database.wishlist.update(new_wishlist)
        return list(wishlist)
    except Exception as e: 
        print(f"Error updating user wishlist from MongoDB: {e}")

# Update user info
def update_user_info(mongodb_database, email, updated_info):
    try:
        mongodb_database.users.update_one(
            {"email": email},
            {"$set": updated_info}
        )
    except Exception as e:
        print(f"Error updating user info in MongoDB: {e}")
        raise

# Return request
def add_return_request(mongodb_database, order_id, product_id, reason):
    try:
        return_request = {
            "order_id": order_id,
            "product_id": product_id,
            "reason": reason,
            "status": "Pending",
            "request_date": datetime.now()
        }
        mongodb_database.returns.insert_one(return_request)
    except Exception as e:
        print(f"Error inserting return request into MongoDB: {e}")
        raise

