#!/usr/bin/env python3
import os
from datetime import datetime
import uuid
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from pymongo import MongoClient
from cassandra.cluster import Cluster
from pydgraph import DgraphClient, DgraphClientStub
from Models import mongoModel, cassandraModel, dgraphModel


# Database Configuration
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGODB_DB_NAME", "app")

CASSANDRA_URI = os.getenv("CASSANDRA_URI", "127.0.0.1")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "app")

DGRAPH_URI = os.getenv("DGRAPH_URI", "localhost:9080")

# FastAPI instance
app = FastAPI()

# Database instances
@app.on_event("startup")
def startup_db_client():
    # MongoDB
    client = MongoClient(MONGODB_URI)
    app.mongodb_database = client[DB_NAME]
    print(f"Connected to MongoDB at: {MONGODB_URI} \n\t Database: {DB_NAME}")
    
    # Cassandra
    cluster = Cluster([CASSANDRA_URI])
    app.cassandra_session = cluster.connect()
    app.cassandra_session.set_keyspace(CASSANDRA_KEYSPACE)
    print(f"Connected to Cassandra at: {CASSANDRA_URI} \n\t Keyspace: {CASSANDRA_KEYSPACE}")
    
    # Dgraph
    app.dgraph_client = DgraphClient(DgraphClientStub(DGRAPH_URI))
    print(f"Connected to Dgraph at: {DGRAPH_URI}")

@app.on_event("shutdown")
def shutdown_db_client():
    # Close MongoDB connection
    app.mongodb_database.client.close()
    print("Closed MongoDB connection.")
    
    # Close Cassandra connection
    app.cassandra_session.cluster.shutdown()
    print("Closed Cassandra connection.")
    
    # Close Dgraph connection
    app.dgraph_client.close()
    print("Closed Dgraph connection.")



# Register User Endpoint
@app.post("/register", status_code=201)
def register_user(user: mongoModel.User):
    db = app.mongodb_database
    if mongoModel.get_mongo_user(db, user.email):
        raise HTTPException(status_code=400, detail="Email already registered")
    
    user_data = user.dict()  
    mongoModel.add_mongo_user(db, user_data)
    
    return {"message": "User registered successfully", "email": user.email, "username": user.username}

@app.post("/login", status_code=200)
def login_user(user: mongoModel.UserLogin):
    db = app.mongodb_database
    user_data = user.dict()
    mongoresponse=mongoModel.verify_mongo_user(db, user_data["email"], user_data["password"])
    if mongoresponse:
        return {"message": "User login successfully", "email": mongoresponse["email"], "username": mongoresponse["username"]}
    else:
        raise HTTPException(status_code=400, detail="Email or password incorrect")

@app.put("/users/{email}", status_code=200)
def update_user_profile(email: str, updates: dict):
    try:
        db = app.mongodb_database
        updated = mongoModel.update_mongo_user(db, email, updates)
        if updated:
            return {"message": "User profile updated successfully"}
        else:
            raise HTTPException(status_code=404, detail="User not found or no changes made")
    except Exception as e:
        print(f"Error in update_user_profile: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    
@app.get("/profiledetails/{email}", status_code=200)
def get_profile_details(email: str):
    db = app.mongodb_database
    profile = mongoModel.get_mongo_user(db, email)
    if profile:
        return profile
    else:
        raise HTTPException(status_code=404, detail="User not found")

    
@app.post("/addproduct/{email}", status_code=200)
def add_product(email: str, product: mongoModel.Product):
    try:
        db = app.mongodb_database
        dictio = product.dict()
        productAdded = mongoModel.add_product(db, dictio)
        if productAdded:
            addToSeller=mongoModel.add_product_to_seller(db, email, productAdded["_id"])
            if(addToSeller):
                return "Success"
            else:
                return "Failed"
        else:
            raise HTTPException(status_code=404, detail="Could not add product")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/myproducts/{email}", status_code=200)
def my_products(email: str): 
    try:
        db = app.mongodb_database
        user_data = mongoModel.get_mongo_user(db, email)  
        if not user_data:
            raise HTTPException(status_code=404, detail="User not found") 
        
        userProductsID = user_data["products"]
        
        if userProductsID:
            listOfProducts = []
            for product_id in userProductsID:
                product = mongoModel.find_product(db, product_id)
                if product:
                    listOfProducts.append(product)  
                else:
                    listOfProducts.append(product)  
            return listOfProducts  
        else:
            return "User has no products"  
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error: " + str(e))
    
from fastapi import HTTPException, APIRouter
from pymongo import MongoClient


@app.put("/editproduct/{pid}", status_code=200)
def editProduct(pid: str, newProduct: dict):
    try:
        db = app.mongodb_database
        update_result = mongoModel.update_product(db, pid, newProduct)
        
        if update_result:
            return {"message": "Product updated successfully"}
        else:
            raise HTTPException(status_code=404, detail=update_result)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error: " + str(e))

@app.delete("/deleteproduct/{pid}", status_code=200)
def deleteProduct(pid: str, email: str):
    try:
        db = app.mongodb_database
        product_removed = mongoModel.remove_product_from_seller(db, pid, email)
        if not product_removed:
            raise HTTPException(status_code=404, detail="Product not found or could not be removed from user.")
        
        product_deleted = mongoModel.remove_product(db, pid)
        if product_deleted.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Product not found in products collection.")
        
        return {"message": "Product deleted successfully"}
    except Exception as e:
        print(f"Error in deleteProduct: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/products")
def get_products(page: int , page_size: int ):
    try:
        db = app.mongodb_database  
        skip = (page - 1) * page_size

        products = mongoModel.show_product_list(db, skip, page_size)
        if not products:
            raise HTTPException(status_code=404, detail="No products found for this page.")

        return products
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")
    
@app.get("/search", status_code=200)
def search_products(q: str):
    db= app.mongodb_database
    try:
        # Realizar la búsqueda en la base de datos usando mongoModel
        products = mongoModel.search_products(db, q)
        return products
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")