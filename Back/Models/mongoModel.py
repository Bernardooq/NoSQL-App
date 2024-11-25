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

class Wishlist(BaseModel):
    email: str
    products: list = []

class Cart(BaseModel):
    _id: str
    products: list = []


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
                "products": [str(product_id) for product_id in user["products"]]
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


def remove_product_from_seller(mongodb_database, product_id, user_email):
    try:
        user_email = str(user_email)
        product_id = ObjectId(product_id)  

        result = mongodb_database.users.update_one(
            {"email": user_email},
            {"$pull": {"products": product_id}}
        )

        print(f"Modified count: {result.modified_count}")
        return result.modified_count > 0
    except Exception as e:
        print(f"Error removing product from seller: {e}")
        return False


def remove_product(mongodb_database, product_id):
    try:
        result = mongodb_database.products.delete_one({"_id": ObjectId(product_id)})
        return result
    except Exception as e:
        print(f"Error deleting product: {e}")
        return False

def update_product(mongodb_database, product_id, productUpdate):
    product=find_product(mongodb_database, product_id)
    if product:
        product_id= ObjectId(product_id)
        updated_product = mongodb_database.products.update_one(
            {"_id": product_id},
            {"$set": productUpdate}
        )       
        if updated_product.matched_count > 0:
            return True
        else:
            return False
    else:
        return {"message": "Product not found"}


def find_product(mongodb_database, product_id):
    if not isinstance(product_id, ObjectId): 
        product_id = ObjectId(product_id)   
    product = mongodb_database.products.find_one({"_id": product_id})
    if product:
        return {
            "product_id": str(product["_id"]),
            "name": product["name"],
            "description": product["description"],
            "price": product["price"],
            "image": product["image"]
        }
    else:
        return None


def show_product_list(mongodb_database, skip=None, pagesize=None):
        if(skip is not None and pagesize is not None):
            products_cursor = (
                mongodb_database.products.find()
                .skip(skip)
                .limit(pagesize)
            )
            products = [
                {
                    "product_id": str(product["_id"]),
                    "name": product["name"],
                    "description": product["description"],
                    "price": product["price"],
                    "image": product["image"],
                    "_id": str(product["_id"]),
                }
                for product in products_cursor
            ]

            return products
        else:
            products= mongodb_database.products.find()
            return products


def search_products(mongodb_database, query):
    try:
        search_regex = {"$regex": query, "$options": "i"} 
        products_cursor = mongodb_database.products.find(
            {"$or": [{"name": search_regex}, {"description": search_regex}]}
        )

        products = [
            {
                "name": product["name"],
                "description": product["description"],
                "price": product["price"],
                "image": product["image"],
                "_id": str(product["_id"]),
            }
            for product in products_cursor
        ]
        return products
    except Exception as e:
        print(f"Error searching for products: {e}")
        return []
   


def remove_from_cart(mongodb_database, email, product_id):
    try:
        if not isinstance(product_id, ObjectId):
            product_id = ObjectId(product_id)

        result = mongodb_database.carts.update_one(
            {"email": email},
            {"$pull": {"products": {"product_id": product_id}}}
        )
        if result.modified_count > 0:
            return True
        else:
            return False
    except Exception as e:
        print(f"Error removing from cart: {e}")
        return False

    
def edit_cart(mongodb_database, email, product_id, new_quantity):
    try:
        if not isinstance(product_id, ObjectId):
            product_id = ObjectId(product_id)

        if int(new_quantity) <= 0:
            return remove_from_cart(mongodb_database, email, product_id)

        result = mongodb_database.carts.update_one(
            {"email": email, "products.product_id": product_id},
            {"$set": {"products.$.quantity": new_quantity}}
        )
        if int(result.modified_count) > 0:
            return True
        else:
            return False
    except Exception as e:
        print(f"Error editing cart: {e}")
        return False


def view_cart(mongodb_database, email):
    try:
        cart = mongodb_database.carts.find_one({"email": email})
        if not cart or not cart.get("products"):
            return {"items": []}

        product_ids = [product["product_id"] for product in cart["products"]]
        products = mongodb_database.products.find({"_id": {"$in": product_ids}})

        cart_details = []
        for product in cart["products"]:
            product_info = next((p for p in products if p["_id"] == product["product_id"]), {})
            cart_details.append({
                "product_id": str(product["product_id"]),
                "name": product_info.get("name", "Unknown"),
                "price": product_info.get("price", 0.0),
                "quantity": product["quantity"]
            })

        return {"items": cart_details}
    except Exception as e:
        print(f"Error viewing cart: {e}")
        return {"items": []} 


def add_to_cart(mongodb_database, email, product_id, quantity=1):
    try:
        if not isinstance(product_id, ObjectId):
            product_id = ObjectId(product_id)
        cart = mongodb_database.carts.find_one({"email": email})
        if not cart:
            cart = {"email": email, "products": []}
            mongodb_database.carts.insert_one(cart)

        existing_product = next((product for product in cart["products"] if product["product_id"] == product_id), None)

        if existing_product:
            new_quantity = existing_product["quantity"] + quantity
            result = mongodb_database.carts.update_one(
                {"email": email, "products.product_id": product_id},
                {"$set": {"products.$.quantity": new_quantity}}
            )
            if result.modified_count > 0:
                return {"message": "Product quantity updated successfully."}
            else:
                return {"message": "Failed to update product quantity."}
        else:
            result = mongodb_database.carts.update_one(
                {"email": email},
                {"$push": {"products": {"product_id": product_id, "quantity": quantity}}}
            )
            if result.modified_count > 0:
                return {"message": "Product added to cart successfully."}
            else:
                return {"message": "Failed to add product to cart."}

    except Exception as e:
        print(f"Error adding product to cart: {e}")
        return {"message": "Error adding product to cart."}


# Insert product at wishlist
def add_to_wishlist(mongodb_database, email, product_id):
    try:
        if not isinstance(product_id, ObjectId):
            product_id = ObjectId(product_id) 
        if not mongodb_database.products.find_one({"_id": product_id}): return False
        wishlist = mongodb_database.wishlist.find_one({"email": email})
        if not wishlist:
            mongodb_database.wishlist.insert_one({
                "email": email,
                "products": [{"product_id": product_id}]
            })
            return {"message": "Product added to wishlist."}
        existing_product = next((p for p in wishlist["products"] if p["product_id"] == product_id), None)
        if existing_product:
            return {"message": "Product already added to wishlist"}
        
        result = mongodb_database.wishlist.update_one(
            {"email": email},
            {"$push": {"products": {"product_id": product_id}}}
        )

        if result.modified_count > 0:
            return {"message": "Product added to wishlist."}
        else:
            return False
    
    except Exception as e:
        print(f"Error adding to wishlist: {e}")
        return False

# Get user wishlist
def get_user_wishlist(mongodb_database, email):
    try:
        wishlist = mongodb_database.wishlist.find_one({"email": email})
        if not wishlist or not wishlist.get("products"):
            return {"items": []}  
        product_ids = [product["product_id"] for product in wishlist["products"]]
        
        products = list(mongodb_database.products.find({"_id": {"$in": product_ids}}))

        wishlist_details = []
        for product in wishlist["products"]:
            product_info = next((p for p in products if str(p["_id"]) == str(product["product_id"])), None)
            if product_info:
                wishlist_details.append({
                    "product_id": str(product["product_id"]),
                    "name": product_info.get("name", "Unknown"),
                    "price": product_info.get("price", 0.0),
                    "description": product_info.get("description", "No description available"),
                })

        return {"items": wishlist_details}  
    except Exception as e:
        print(f"Error viewing wishlist: {e}")
        return {"items": []}  


def delete_from_wishlist(mongodb_database, email, product_id):
    try:
        if not isinstance(product_id, ObjectId):
            product_id = ObjectId(product_id)
        wishlist = mongodb_database.wishlist.find_one({"email": email})

        if not wishlist:
            return {"message": "Wishlist not found."}

        result = mongodb_database.wishlist.update_one(
            {"email": email},
            {"$pull": {"products": {"product_id": product_id}}}
        )

        if result.modified_count > 0:
            return {"message": "Product removed from wishlist."}
        else:
            return False

    except Exception as e:
        print(f"Error deleting from wishlist: {e}")
        return False

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
    except Exception as e:
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
    except Exception as e:
        print(f"Error inserting return request into MongoDB: {e}")
        raise

