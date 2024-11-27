#!/usr/bin/env python3
import datetime
import json

import pydgraph



def set_schema(client):
    schema = """
        type User {
            user_id
            reviewed_products
        }
        type Product {
            product_id
            recommended_for
            related_products
            reviews
            rating
        }
        type Suggestions {
            user_id
            suggestions        
        }
        type Feedback {
            user_id
            recommendation_feedback
        }

        user_id: string @index(exact).
        reviewed_products: [uid] @reverse . # Edge to Product with facets {rating, review_text}

        product_id: string @index(exact) .
        recommended_for: [uid] @reverse . # Edge to User with facet {score}
        related_products: [uid] @reverse . # Edge to Product
        reviews: [uid] . # Edge to Product
        rating: int . # Edge to Product

        suggestions: [uid] @reverse . # Edge to Product with facet {score}

        recommendation_feedback: [uid] @reverse .
    """
    return client.alter(pydgraph.Operation(schema=schema))


def create_data(client):
    # Create a new transaction.
    txn = client.txn()
    try:
        p = {
            'uid': '_:leo',
            'user_id': '2' ,
            'reviewed_products': {
                'product_id':'1',
            }
        }

        response = txn.mutate(set_obj=p)

        # Commit transaction.
        commit_response = txn.commit()
        print(f"Commit Response: {commit_response}")

        print(f"UIDs: {response.uids}")
    finally:
        # Clean up. 
        # Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()


def delete_person(client, name):
    # Create a new transaction.
    txn = client.txn()
    try:
        query1 = """query search_user($a: string) {
            all(func: eq(user_id, $a)) {
                user_id
                reviewed_products {
                    product_id
                    
                }
            }
        }"""
        variables = {'$a': name}
        result = txn.query(query1, variables=variables)
        ppl = json.loads(result.json)
        for person in ppl['all']:
            print("UID: " + person['uid'])
            txn.mutate(del_obj=person)
            print(f"{name} deleted")
        commit_response = txn.commit()
        print(commit_response)
    finally:
        txn.discard()


def search_person(client, name):
    query = """query search_user($a: string) {
        all(func: eq(user_id, $a)) {
            user_id
            reviewed_products {
                product_id
            }
        }
    }"""

    variables = {'$a': name}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)
    unique_users = {}
    for User in ppl['all']:
        unique_users[User['user_id']] = User
    # Print results.
    print(f"Data associated with {name}:\n{json.dumps(unique_users, indent=2)}")


def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))
import csv


# Establish Dgraph client
client_stub = pydgraph.DgraphClientStub('localhost:9080')
client = pydgraph.DgraphClient(client_stub)

def load_products(file_path):
    txn = client.txn()
    resp = None
    try:
        products = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                products.append({
                    'uid': '_:' + row['product_id'],
                    'product_id':row['product_id'],
                    'related_product': row['related_product'],
                    'rating':row['rating']
                })
            resp = txn.mutate(set_obj=products)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids

def load_suppliers(file_path):
    txn = client.txn()
    resp = None
    try:
        suppliers = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                suppliers.append({
                    'uid': '_:' + row['user_id'],
                    'feedback': row['feedback']
                })
            resp = txn.mutate(set_obj=suppliers)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids

def load_users(file_path):
    txn = client.txn()
    resp = None
    try:
        products = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                products.append({
                    'uid': '_:' + row['user_id'],
                    'user_id':row['user_id'],
                })
            resp = txn.mutate(set_obj=products)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids

def load_feedback(file_path):
    txn = client.txn()
    resp = None
    try:
        products = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                products.append({
                    'uid': '_:' + row['product_id'],
                    'product_id':row['product_id'],
                    'feedback': row['feedback']
                    
                })
            resp = txn.mutate(set_obj=products)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids

def append_to_csv(product_id, feedback):
    file_name="./Models/feedback.csv"
    with open(file_name, mode='a', newline='') as file:
        fieldnames = ['product_id', 'feedback']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writerow({
            'product_id': product_id,
            'feedback': feedback
        })
    product_uids = load_products('./Models/products.csv')
    feedback_uids=load_feedback('./Models/feedback.csv')
    create_edges_reviews('./Models/feedback.csv',product_uids, feedback_uids)


# Function to create edges between products and suppliers
def create_edges_related(file_path, product_uids, supplier_uids): #related
    txn = client.txn()
    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                supplier= row['product_id']
                product = row['related_product']
                mutation = {
                    'uid': supplier_uids[supplier],
                    'related_products': {
                        'related_product_id': product_uids[product]
                    }
                }
                txn.mutate(set_obj=mutation)
        txn.commit()
    finally:
        txn.discard()


def create_edges_reviews(file_path, product_uids, feedback_uids): # reviews
    txn = client.txn()
    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                product = row['product_id']
                feedback_text = row['feedback']
                mutation = {
                    'uid': product_uids[product],
                    'reviews': [
                        {
                            'feedback': feedback_text
                        }
                    ]
                }
                txn.mutate(set_obj=mutation)  # Mutate the reviews field with feedback
        txn.commit()
    finally:
        txn.discard()


def create_edges_recommended(file_path, product_uids, supplier_uids): #recommended
    txn = client.txn()
    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                supplier= row['product_id']
                product = row['recommended_forus']
                mutation = {
                    'uid': supplier_uids[supplier],
                    'recommended_for': {
                        'user_id': product_uids[product]
                    }
                }
                txn.mutate(set_obj=mutation)
        txn.commit()
    finally:
        txn.discard()

def create_edges_reviewed(file_path, product_uids, user_uids): #reviewed
    txn = client.txn()
    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                users= row['user_id']
                product = row['product_id']
                mutation = {
                    'uid': user_uids[users],
                    'reviewed_products': {
                        'uid': product_uids[product]
                    }
                }
                txn.mutate(set_obj=mutation)
        txn.commit()
    finally:
        txn.discard()


def product_query_reviews(client, id):
    query = """
    query getRelatedProducts($productId: string) {
        reviews(func: eq(product_id, $productId)) {
            product_id
            reviews {
                feedback
            }
        }
    }
    """
    variables = {'$productId': id}
    # Execute query
    res = client.txn(read_only=True).query(query, variables=variables)
    
    # Decode and load JSON response
    raw_json = res.json.decode('utf-8') if isinstance(res.json, bytes) else res.json
    data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    # Extract related_product_id values
    related_ids = []
    for product in data.get("reviews", []):
        for review in product.get("reviews", []):
            related_id = review.get("feedback")
            if related_id:
                related_ids.append(related_id)

    sets=set(related_ids)
    print(sets)



def product_query_related(client, id):
    query = """
    query getRelatedProducts($productId: string) {
        related_products(func: eq(product_id, $productId)) {
            product_id
            related_products {
                related_product_id
            }
        }
    }
    """
    variables = {'$productId': id}
    # Execute query
    res = client.txn(read_only=True).query(query, variables=variables)
    
    # Decode and load JSON response
    raw_json = res.json.decode('utf-8') if isinstance(res.json, bytes) else res.json
    data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    
    # Print the raw data
    print("Raw data from query:")
    print(json.dumps(data, indent=2))
    # Extract related_product_id values
    related_ids = []
    for product in data.get("related_products", []):
        for related in product.get("related_products", []):
            related_id = related.get("related_product_id")
            if related_id:
                related_ids.append(related_id)

    sets=set(related_ids)
    print(related_ids)
    print(sets)


def product_query_rating(client, id):
    query = """
    query getRelatedProducts($productId: string) {
        related_products(func: eq(product_id, $productId)) {
            product_id
            rating
        }
    }
    """
    variables = {'$productId': id}
    # Execute query
    res = client.txn(read_only=True).query(query, variables=variables)
    
    # Decode and load JSON response
    raw_json = res.json.decode('utf-8') if isinstance(res.json, bytes) else res.json
    data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    
    ratings = []
    for product in data.get("related_products", []):
        rating = product.get("rating")
        if rating is not None:
            ratings.append(rating)

    sets=set(ratings)
    print(sets)



# Call your create schema function here
set_schema(client)

# Load products and suppliers
product_uids = load_products('./Models/products.csv')
user_uids=load_users('./Models/users.csv')
feedback_uids=load_feedback('./Models/feedback.csv')
# Create relationships
create_edges_related('./Models/products.csv', product_uids, product_uids)
create_edges_reviewed('./Models/reviews.csv', product_uids, user_uids)
create_edges_recommended('./Models/products.csv', product_uids, product_uids)
create_edges_reviews('./Models/feedback.csv',product_uids, feedback_uids)
# Call your query functions here
