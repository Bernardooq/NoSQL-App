# FastAPI Server Project with Database Connections

This project uses FastAPI to create a server that connects to various databases like MongoDB, Dgraph, and Cassandra.

## 1. Create the Server in Python

To create the server, use FastAPI. Here is a basic example:

```python
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Hello, World!"}

# Start the server with `uvicorn`
# Command in terminal: uvicorn main:app --reload
```

## 2. Connecting to Databases

### a. Connecting to MongoDB

To connect your application to MongoDB, use PyMongo. Here’s an example:

```python
from pymongo import MongoClient

# Conectar a MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["nombre_de_tu_base_de_datos"]
collection = db["nombre_de_tu_colección"]
```

### b. Connecting to Dgraph

To connect to Dgraph, use the pydgraph library. Make sure to have the client installed:

```
pip install pydgraph
```

Then you can connect like this:

```python
import pydgraph

client_stub = pydgraph.DgraphClientStub('localhost:9080')
client = pydgraph.DgraphClient(client_stub)
```

### c. Connecting to Cassandra

For Cassandra, use the cassandra-driver. Here’s how to connect:

```
pip install cassandra-driver
```

And the code to connect would be:

```python
from cassandra.cluster import Cluster

# Connect to Cassandra
cluster = Cluster(['localhost'])
session = cluster.connect('your_keyspace_name')
```

## 3. Project Structure (Back)

```
/Back
│
├── main.py                  # Main server file
├── routers/                 # API routes
│   ├── __init__.py
│   ├── mongo_routes.py      # Routes for MongoDB
│   ├── dgraph_routes.py     # Routes for Dgraph
│   └── cassandra_routes.py  # Routes for Cassandra
├── models/                  # Data models
│   ├── mongo_model.py       # MongoDB model
│   ├── dgraph_model.py      # Dgraph model
│   └── cassandra_model.py   # Cassandra model
└── database/                # Database connections
    ├── mongo_connection.py   # Connection to MongoDB
    ├── dgraph_connection.py  # Connection to Dgraph
    └── cassandra_connection.py # Connection to Cassandra

```

## 4. Summary

* **Server** : Use FastAPI to create the server.
* **Connections** : Use the appropriate libraries to connect to each database.
* **Structure** : Organize your code into files and folders based on functionality.

## 5. Running the Server

To run the server, make sure you have all dependencies installed. You can use a `requirements.txt` file to manage your dependencies:

```
fastapi
uvicorn
pymongo
pydgraph
cassandra-driver

```

Install the dependencies using:

```
pip install -r requirements.txt
```

Finally, run the server:

```
uvicorn main:app --reload
```
