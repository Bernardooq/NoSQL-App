from pydgraph import DgraphClient, DgraphClientStub

# Conexión con Dgraph
def init_dgraph(dgraph_uri):
    stub = DgraphClientStub(dgraph_uri)
    client = DgraphClient(stub)
    return client

# Insertar usuario en Dgraph
def insert_dgraph_user(app, user_id, name, email):
    try:
        # Construir el mutación para insertar datos
        mutation = {
            "uid": "_:user",
            "user_id": user_id,
            "name": name,
            "email": email
        }
        txn = app.dgraph_client.txn()
        txn.mutate(set_json=mutation)
        txn.commit()
    except Exception as e:
        print(f"Error inserting user into Dgraph: {e}")
        raise

# Obtener usuario desde Dgraph
def get_dgraph_user(app, user_id):
    try:
        query = f"""
        {{
            user(func: eq(user_id, {user_id})) {{
                user_id
                name
                email
            }}
        }}
        """
        result = app.dgraph_client.txn().query(query)
        return result.json
    except Exception as e:
        print(f"Error getting user from Dgraph: {e}")
        raise
