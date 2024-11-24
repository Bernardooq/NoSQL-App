#!/usr/bin/env python3
import os
from fastapi import FastAPI
from pymongo import MongoClient
from cassandra.cluster import Cluster
from pydgraph import DgraphClient, DgraphClientStub
from routes import router as r  
from Models.mongoModel import init_mongo 
from Models.cassandraModel import init_cassandra  
from Models.dgraphModel import init_dgraph 

# Configure environment
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGODB_DB_NAME", "app")

CASSANDRA_URI = os.getenv("CASSANDRA_URI", "127.0.0.1")  
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "app")

DGRAPH_URI = os.getenv("DGRAPH_URI", "localhost:9080") 

app = FastAPI()

@app.on_event("startup")
def startup_db_client():
    # Database connections
    app.mongodb_client, app.mongodb_database = init_mongo(MONGODB_URI, DB_NAME)
    print(f"Connected to MongoDB at: {MONGODB_URI} \n\t Database: {DB_NAME}")
    
    app.cassandra_session = init_cassandra(CASSANDRA_URI, CASSANDRA_KEYSPACE)
    print(f"Connected to Cassandra at: {CASSANDRA_URI} \n\t Keyspace: {CASSANDRA_KEYSPACE}")
    
    app.dgraph_client = init_dgraph(DGRAPH_URI)
    print(f"Connected to Dgraph at: {DGRAPH_URI}")

@app.on_event("shutdown")
def shutdown_db_client():
    app.mongodb_client.close()
    print("Closed MongoDB connection.")
    
    app.cassandra_session.cluster.shutdown()
    print("Closed Cassandra connection.")
    
    app.dgraph_client.close()
    print("Closed Dgraph connection.")

# Include your routes here
app.include_router(r, tags=["app"], prefix="/app")
