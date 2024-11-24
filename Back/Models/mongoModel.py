import random
import uuid
from pydantic import BaseModel, Field
from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId


class Product(BaseModel):
    name: str
    description: str
    price: float
    category: str
    image: str

class User(BaseModel):
    username: str 
    email: str
    password: str
    created_at: datetime = datetime.now()
    address: str = None
    products: list = []

class UserLogin(BaseModel):
    email: str
    password: str

# Mongo connection
def init_mongo(mongo_uri, db_name):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    return client, db

# Insert User
def add_mongo_user(mongodb_database, user_data):
    try:
        mongodb_database.users.insert_one(user_data)
    except Exception as e:
        print(f"Error inserting user into MongoDB: {e}")
        raise

def verify_mongo_user(mongodb_database, email, password):
    try:
        response= mongodb_database.users.find_one({"email": email, "password": password})
        return response
    except Exception as e:
        print(f"Error user not found: {e}")
        raise

# Get User
def get_mongo_user(mongodb_database, email):
    try:
        user = mongodb_database.users.find_one({"email": email})
        if user:
            return {
                "_id": str(user["_id"]),
                "email": user["email"],
                "username": user["username"],
                "address": user["address"], 
                "created_at": user["created_at"], 
                "products": user["products"],
            }
        return None
    except Exception as e:
        print(f"Error fetching user profile: {e}")
        raise

def update_mongo_user(mongodb_database, email, new_user):
    try:
        result = mongodb_database.users.update_one(
            {"email": email}, 
            {"$set": new_user} 
        )
        return result.modified_count > 0  
    except Exception as e:
        print(f"Error updating user: {e}")
        raise
# Product catalog
def add_product(mongodb_database, product):
    try:
        mongodb_database.products.insert_one(product)
        return product
    except Exception as e:
        print(f"Error inserting product into MongoDB: {e}")
        raise

def add_product_to_seller(mongodb_database, user_email, product_id):
    try:
        user = mongodb_database.users.find_one({"email": user_email})
        if not user:
            print("User not found")
            return None

        result = mongodb_database.users.update_one(
            {"email": user_email},
            {"$push": {"products": product_id}}
        )
        if result.modified_count > 0:
            print(f"Product with ID {product_id} added to user {user_email}")
            return True
        else:
            print("No changes made to the user's product list")
    except Exception as e:
        print(f"Error adding product to seller: {e}")
        raise


def remove_product_from_seller(mongodb_database, user_email, product_id):
    try:
        user = mongodb_database.users.find_one({"email": user_email})
        if not user:
            print("User not found")
            return None
        result = mongodb_database.users.update_one(
            {"email": user_email},
            {"$pull": {"products": product_id}}  
        )
        if result.modified_count > 0:
            print(f"Product with ID {product_id} removed from user {user_email}")
        else:
            print("No changes made to the user's product list")
    except Exception as e:
        print(f"Error removing product from seller: {e}")
        raise

def edit_product(mongodb_database, user_email, product_id, updated_product_data):
    try:
        user = mongodb_database.users.find_one({"email": user_email})
        if not user:
            print("User not found")
            return None
        
        if product_id not in [str(p) for p in user.get('products', [])]:  # Convertimos los ids a string
            print(f"Product with ID {product_id} does not belong to user {user_email}")
            return None
        
        product = mongodb_database.products.find_one({"_id": product_id})
        if not product:
            print(f"Product with ID {product_id} not found in products collection")
            return None

        result = mongodb_database.products.update_one(
            {"_id": product_id},  # Usamos ObjectId para la búsqueda
            {"$set": updated_product_data}  # Usamos $set para actualizar los campos
        )

        if result.modified_count > 0:
            print(f"Product with ID {product_id} updated successfully")
        else:
            print("No changes made to the product")
    except Exception as e:
        print(f"Error editing product: {e}")
        raise


def find_product(mongodb_database, product_id):
    try:
        if not isinstance(product_id, ObjectId): 
            product_id = ObjectId(product_id)
        product = mongodb_database.products.find_one({"_id": product_id})
        return product
    except Exception as e:
        print(f"Error finding product: {e}")
        return None


def update_product(mongodb_database, product_id, newproduct):
    try:
        product= mongodb_database.products.update_one({"_id": product_id,}, 
            {"$set": newproduct})
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

from pymongo.errors import PyMongoError

def add_to_cart(mongodb_database, email, product):
    try:
        cart_item = mongodb_database.carts.find_one({"email": email, "product_id": product["product_id"]})
        
        if cart_item:
            mongodb_database.carts.update_one(
                {"email": email, "product_id": product["product_id"]},
                {"$inc": {"quantity": product.get("quantity", 1)}}
            )
        else:
            new_item = {
                "email": email,
                "product_id": product["product_id"],
                "product_name": product["product_name"],
                "quantity": product.get("quantity", 1),
                "price": product["price"]
            }
            mongodb_database.carts.insert_one(new_item)
    except PyMongoError as e:
        print(f"Error adding product to cart in MongoDB: {e}")
        raise

# Get User shopping cart
def get_user_cart(mongodb_database, email):
    try:
        cart_items = list(mongodb_database.carts.find({"email": email}))
        return cart_items  
    except PyMongoError as e:
        print(f"Error getting user cart from MongoDB: {e}")
        raise

# Edit User shopping cart
def edit_user_cart(mongodb_database, email, new_cart):
    try:
        if new_cart["quantity"] == 0:
            mongodb_database.carts.delete_one({"email": email, "product_id": new_cart["product_id"]})
        else:
            mongodb_database.carts.update_one(
                {"email": email, "product_id": new_cart["product_id"]},
                {"$set": {"quantity": new_cart["quantity"]}}
            )
    except PyMongoError as e:
        print(f"Error updating user cart in MongoDB: {e}")
        raise


# Insert product at wishlist
def add_to_wishlist(mongodb_database, email, product):
    try:
        wishlist_item = {
            "email": email,
            "product_id": product,
            "date_added": datetime.now()
        }
        mongodb_database.wishlist.insert_one(wishlist_item)
    except Exception as e:
        print(f"Error adding product to wishlist in MongoDB: {e}")
        raise

from pymongo.errors import PyMongoError
from datetime import datetime

# Get user wishlist
def get_user_wishlist(mongodb_database, email):
    try:
        wishlist_items = list(mongodb_database.wishlist.find({"email": email}))
        return wishlist_items
    except PyMongoError as e:
        print(f"Error getting user wishlist from MongoDB: {e}")
        raise

# Edit user wishlist
def edit_user_wishlist(mongodb_database, email, product):
    try:
        wishlist_item = mongodb_database.wishlist.find_one({"email": email, "product_id": product["product_id"]})

        if wishlist_item:
            return("Already on the list")
        else:
            new_item = {
                "email": email,
                "product_id": product["product_id"],
                "product_name": product["product_name"],
                "added_date": datetime.now()
            }
            mongodb_database.wishlist.insert_one(new_item)
    except PyMongoError as e:
        print(f"Error updating user wishlist in MongoDB: {e}")
        raise

def delete_from_wishlist(mongodb_database, email, product_id):
    try:
        result = mongodb_database.wishlist.delete_one({"email": email, "product_id": product_id})
        if result.deleted_count > 0:
            return "Product successfully removed from wishlist."
        else:
            return "Product not found in the wishlist."
    except PyMongoError as e:
        print(f"Error deleting product from wishlist in MongoDB: {e}")
        raise

# Update user info
def update_user_info(mongodb_database, email, updated_info):
    """
    Actualiza la información de un usuario basado en su correo electrónico.
    """
    try:
        result = mongodb_database.users.update_one(
            {"email": email},
            {"$set": updated_info}
        )
        if result.matched_count == 0:
            print(f"No user found with email: {email}")
    except PyMongoError as e:
        print(f"Error updating user info in MongoDB: {e}")
        raise

# Add return request
def add_return_request(mongodb_database, order_id, product_id, reason):
    """
    Crea una solicitud de devolución para un producto específico en un pedido.
    """
    try:
        return_request = {
            "order_id": order_id,
            "product_id": product_id,
            "reason": reason,
            "status": "Pending",
            "request_date": datetime.now()
        }
        mongodb_database.returns.insert_one(return_request)
    except PyMongoError as e:
        print(f"Error inserting return request into MongoDB: {e}")
        raise

