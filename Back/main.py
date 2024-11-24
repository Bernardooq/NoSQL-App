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
    if mongoModel.find_user(db, user.email):
        raise HTTPException(status_code=400, detail="Email already registered")
    
    user_data = user.dict()  
    mongoModel.add_mongo_user(db, user_data)
    
    return {"message": "User registered successfully", "user": user.email}
