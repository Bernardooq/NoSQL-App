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
                    'name': row['name'],
                    'price': row['price'],
                    'recommended_forus': row['recommended_forus'],
                    'related_product': row['related_product'],
                    'review':row['review'],
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
                    'uid': '_:' + row['feedback_id'],
                    'feedback_id':row['feedback_id'],
                    'feedback': row['feedback'],
                    'product_id':row['product_id']
                })
            resp = txn.mutate(set_obj=products)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids

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
                        'uid': product_uids[product]
                    }
                }
                txn.mutate(set_obj=mutation)
        txn.commit()
    finally:
        txn.discard()


def create_edges_feedback(file_path, product_uids, feedback_uids): #related
    txn = client.txn()
    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                product= row['product_id']
                feed = row['feedback_id']
                mutation = {
                    'uid': product_uids[product],
                    'reviews': {
                        'uid': feedback_uids[feed]
                    }
                }
                txn.mutate(set_obj=mutation)
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
                        'uid': product_uids[product]
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


def product_query(client, name):
    query = """query search_product($a: string) {
        all(func: eq(product_id, $a)) {
            product_id
            recommended_to{
                User_id
            }
            related_products {
                product_id
            }
            review{
                feedback           
            }
            rating
        }
    }"""

    variables = {'$a': name}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)
    unique_products = {}
    unique_products = {product['product_id']: product for product in ppl['all']}

    print(f"Data associated with {name}:\n{json.dumps(unique_products, indent=2)}")



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
create_edges_feedback('./Models/feedback.csv',product_uids, feedback_uids)
# Call your query functions here
