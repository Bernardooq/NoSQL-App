import argparse
import logging
import os
import requests
import pydgraph



DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')
import random
import json
import datetime
import logging
import random

API_URL = "http://127.0.0.1:8000" 
global user
user = {}
#RCONEXION CON CASSANDRA
from cassandra.cluster import Cluster
from Models import mongoModel, cassandraModel, dgraphModel
# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('3_lab.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars releated to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'ecommerce')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')

log.info("Connecting to Cluster")
cluster = Cluster(CLUSTER_IPS.split(','))
session = cluster.connect()

#DGRAPH
def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)


def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)


def close_client_stub(client_stub):
    client_stub.close()

def main_menu():
    session.set_keyspace(KEYSPACE)
        # Init Client Stub and Dgraph Client
    client_stub = create_client_stub()
    client = create_client(client_stub)
    # Create schema
    dgraphModel.set_schema(client)
    while True:
        print("\n--- Welcome to the Store App ---")
        print("1. Profile Management")
        print("   1.1. See Profile")
        print("   1.2. Update Profile")
        print("2. Product Catalog")
        print("   2.1. Browse Products")
        print("   2.2. Search Products")        
        print("   2.3. Wishlist")
        print("3. Orders and Purchases")
        print("   3.1. Create Purchase Order")
        print("   3.2. View Purchase History")
        print("   3.3. Order Tracking")
        print("4. Shopping Cart")
        print("5. Product Management")
        print("   5.1. Sell New Products")
        print("   5.2. My Products")
        print("   5.3. Product Analytics")
        print("   5.4. Update Product")
        print("   5.5. Delete Product")
        print("6. Reviews, Ratings and Feedback")
        print("7. Promotions")
        print("0. Exit")
        
        choice = input("Select an option: ")

        # Profile Management
        if choice == "1.1":
            see_profile()
        elif choice == "1.2":
            update_profile()

        # Product Catalog
        elif choice == "2.1":
            view_product_catalog()
        elif choice == "2.2":
            search_products()
        elif choice == "2.3":
            manage_wishlist()

        # Orders and Purchases
        elif choice == "3.1":
            create_purchase_order()
        elif choice == "3.2":
            view_purchase_history()
        elif choice == "3.3":
            track_orders()

        # Shopping Cart
        elif choice == "4":
            manage_shopping_cart()

        # Product Management
        elif choice == "5.1":
            sell_product()
        elif choice == "5.2":
            view_my_products()
        elif choice == "5.3":
            seller_statics()
        elif choice == "5.4":
            update_product()
        elif choice == "5.5":
            delete_product()

        # Reviews and Ratings
        elif choice == "6":
            leave_reviews_and_ratings()
        elif choice == "7":
            promotion()

        # Exit
        elif choice == "0":
            print("Exiting... Goodbye!")
            break

        else:
            print("Invalid choice. Please try again.")

        

def search_bar(userid):
    rows = cassandraModel.get_all_search_history_by_user(session, userid)
    unique_queries = set() 
    for row in rows:
        unique_queries.add(row.search_query)  
    for query in unique_queries:
        print(query)

def seller_statics():
    view_my_products()
    product = input("Insert your product id to see some details of it: ")
    cassandraModel.get_stock_level_by_product(session, product)
    cassandraModel.get_product_analytics(session, product)

    

def see_profile():
    print("\n--- See Profile ---")
    if not user.get("email"):
        print("User email is not set. Please log in first.")
        return

    try:
        response = requests.get(f"{API_URL}/profiledetails/{user['email']}")
        if response.status_code == 200:
            try:
                profile = response.json()
                print("Profile Details:")
                print(f"Id: {profile['_id']}")
                print(f"Email: {profile['email']}")
                print(f"Username: {profile['username']}")
                print(f"Address: {profile['address']}")
                print(f"Registration Date: {profile['created_at']}")
            except requests.exceptions.JSONDecodeError:
                print("Error decoding JSON response:", response.text)
        else:
            print(f"Failed to fetch profile details. Status: {response.status_code}, Response: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")


def user_registration():
    print("\n--- User Registration ---")
    username = input("Username: ")
    email = input("Email: ")
    password = input("Password: ")

    response = requests.post(f"{API_URL}/register", json={
        "username": username,
        "email": email,
        "password": password
    })
    
    try:
        response_data = response.json()
    except requests.exceptions.JSONDecodeError:
        response_data = response.text or "No response content"

    if response.status_code == 201:
        print("Registration successful!")
        user["email"]=response_data["email"]
        return response_data
    else:
        print("Registration failed:", response_data)
        return False

    
def user_login():
    print("\n--- User Login ---")
    email = input("Email: ")
    password = input("Password: ")
    response = requests.post(f"{API_URL}/login", json={"email": email, "password": password})

    if response.status_code == 200:
        try:
            response_data = response.json()
            if "email" in response_data and "_id" in response_data:  # Validate fields
                user["email"] = response_data["email"]
                user["id"] = response_data["_id"]
                print("Login successful")
                return response_data
            else:
                print("Unexpected response format:", response_data)
                return False
        except requests.exceptions.JSONDecodeError:
            print("Login failed: Invalid JSON response")
            print("Response text:", response.text)
            return False
    else:
        try:
            error_data = response.json()
            print("Login failed:", error_data)
        except requests.exceptions.JSONDecodeError:
            print("Login failed: Invalid JSON response")
            print("Response text:", response.text)
        return False


def update_profile():
    print("\n--- Update Profile (leave blank to keep current) ---")
    new_username = input("New username: ")
    new_password = input("New Password: ")
    address = input("New address: ")

    user_update = {}
    if new_username:
        user_update["username"] = new_username
    if new_password:
        user_update["password"] = new_password
    if address:
        user_update["address"] = address

    response = requests.put(f"{API_URL}/users/{user['email']}", json=user_update)
    if response.status_code == 200:
        print("Profile updated successfully!")
    else:
        print("Failed to update profile:", response.json())


def promotion():
    o = int(input("1. If you want to create a promotion \n2. If you want to see the latest promotions\n"))
    if o == 1:
        code = input("Insert promo code: ")
        dis = int(input("Insert discount percentage: "))
        product_id = input("Insert product id: ")
        start_date = datetime.datetime.now()
        end_date = start_date + datetime.timedelta(days=random.randint(5, 30))
        cassandraModel.insert_promotions(session, code, dis, str(product_id), start_date,  end_date)
    if o == 2:
        code = input("Search the promotions you are looking for: ")
        cassandraModel.get_promotion_details(session, code)

    print("\n--- PROMOTIONS ---")

def view_product_catalog():
    current_page = 1
    page_size = 10
    while True:
        print(f"\n--- Product Catalog (Page {current_page}) ---")
        response = requests.get(f"{API_URL}/products", params={"page": current_page, "page_size": page_size})
        if response.status_code == 200:
            products = response.json()
            if not products:
                print("No products available.")
                break
            for product in products:
                print(f'{product["name"]} - ${product["price"]} -- {product["_id"]} --- Description: {product["description"]}')

            print("\nOptions:")
            print("1. Next Page")
            print("2. Previous Page")
            print("3. Exit")
            choice = input("Enter your choice: ")

            if choice == "1": 
                current_page += 1
            elif choice == "2" and current_page > 1: 
                current_page -= 1
            elif choice == "3":  
                print("Exiting the catalog.")
                break
            else:
                print("Invalid choice. Please try again.")
        else:
            print("Failed to load product catalog:", response.json())
            break




def create_purchase_order():
    print("\n--- Create Purchase Order ---")
    if "id" not in user:
        print("Error: User is not logged in. Please log in first.")
        return

    user_id = user["id"]
    print(f"User ID: {user_id}")

    # Search for a product
    query = input("Enter product name or keyword: ")
    response = requests.get(f"{API_URL}/search", params={"q": query})
    
    if response.status_code == 200:
        products = response.json()
        if not products:
            print("No products found for your query.")
            return

        print("\n--- Available Products ---")
        for i, product in enumerate(products, start=1):
            print(f"{i}. Name: {product['name']}, Price: ${product['price']})")

        try:
            product_choice = int(input("\nSelect a product by number: "))
            if not (1 <= product_choice <= len(products)):
                print("Invalid selection. Please try again.")
                return
        except ValueError:
            print("Invalid input. Please enter a number.")
            return

        selected_product = products[product_choice - 1]
        product_id = selected_product["_id"]
        price = float(selected_product["price"])
    else:
        print("Search failed:", response.json())
        return

    # Gather additional purchase details
    try:
        quantity = int(input("Quantity: "))
        if quantity <= 0:
            print("Quantity must be a positive number.")
            return
    except ValueError:
        print("Invalid input. Please enter a numeric value for quantity.")
        return

    payment_method = input("Payment Method (Card/Paypal): ").strip().capitalize()
    if payment_method not in ["Card", "Paypal"]:
        print("Invalid payment method. Please select 'Card' or 'Paypal'.")
        return

    # Generate order ID
    order_id = str(random.randint(1000, 9999))


            
    # Insert into Cassandra
    try:
        cassandraModel.insert_purchase_order(
            session, 
            user_id, 
            order_id, 
            product_id, 
            quantity, 
            quantity * price, 
            payment_method
        )
        print("\n--- Purchase Order Created Successfully ---")
        print(f"Order ID         : {order_id}")
        print(f"User ID          : {user_id}")
        print(f"Product Name     : {selected_product['name']}")
        print(f"Quantity         : {quantity}")
        print(f"Price per Unit   : ${price:.2f}")
        print(f"Total Price      : ${quantity * price:.2f}")
        print(f"Payment Method   : {payment_method}")
        print("-" * 30)
    except Exception as e:
        print("Failed to create purchase order:", e)
    cassandraModel.increase_orders_and_revenue(session, product_id, quantity, int(price))


def view_purchase_history():
    print("\n--- Purchase History ---")
    cassandraModel.retrieve_user_orders(session, str(user["id"]))
    

def manage_shopping_cart():
    while True:
        print("\n--- Shopping Cart ---")
        email = user["email"]
        response = requests.get(f"{API_URL}/cart/{email}")
        if response.status_code == 200:
            cart = response.json()
            total = 0
            lisIds = []
            if cart["items"]:
                print("\nYour Cart:")
                for i, item in enumerate(cart["items"], start=1):
                    lisIds.append(item['product_id'])
                    total = total + item['quantity'] * item['price']
                    print(f"{i}. {item['name']} x{item['quantity']} - ${item['price']} - UUID: {item['product_id']}")
                 
            else:
                print("\nYour cart is empty.")

            print(total)
        else:
            print("Failed to retrieve cart:", response.json())
            break
        print(lisIds)
        

        print("\n--- Menu ---")
        print("1. Add product to cart")
        print("2. Remove product from cart")
        print("3. Update product quantity in cart")
        print("4. Abandoned Cart")
        print("5. Exit")

        choice = input("Choose an option: ")
        if choice == "1":
            # Search for a product
            query = input("Enter product name or keyword: ")
            response = requests.get(f"{API_URL}/search", params={"q": query})
            
            if response.status_code == 200:
                products = response.json()
                if not products:
                    print("No products found for your query.")
                    return

                print("\n--- Available Products ---")
                for i, product in enumerate(products, start=1):
                    print(f"{i}. Name: {product['name']}, Price: ${product['price']})")

                try:
                    product_choice = int(input("\nSelect a product by number: "))
                    if not (1 <= product_choice <= len(products)):
                        print("Invalid selection. Please try again.")
                        return
                except ValueError:
                    print("Invalid input. Please enter a number.")
                    return

                selected_product = products[product_choice - 1]
                pid = selected_product["_id"]
                print(pid)
            newresponse = requests.post(f"{API_URL}/addtocart/{pid}/{email}")

            #ADD TO ABANDONED CART 
            
        elif choice == "2":
            pid= input("Product ID: ")
            newresponse = requests.delete(f"{API_URL}/deletefromcart/{pid}/{email}")
        elif choice == "3":
            pid= input("Product ID: ")
            newamount= int(input("New amount: "))
            newresponse = requests.put(f"{API_URL}/editcart/{pid}/{newamount}/{email}")
        elif choice == "4":
            op = int(input("1. Abandon actual cart\n2. See abandoned cart\n"))
            if op == 1:
                print("in the delete")
                cassandraModel.insert_abandoned_cart(session, str(user["id"]), lisIds, total)
                newresponse = requests.delete(f"{API_URL}/deletecart/{user['email']}")
                print("DDLETED")
            if op == 2:
                print("CART ABANDONED")
                abandoned_carts = cassandraModel.get_abandoned_carts_by_user(session, user["id"])
                idList =  []
                for cart in abandoned_carts:
                    print(f"Items: {cart['items']}")
                    print(f"Total Value: ${cart['value']}")
                    print(f"Time Abandoned: {cart['time']}")
                    print("-" * 40)
                    idList.extend(cart['items'])

                print(idList[0])
                idp = idList[0]
                newresponse = requests.get(f"{API_URL}/product/{idp}")
                
                # newresponse = requests.get(f"{API_URL}/product/{id}")
                # print(newresponse)
        elif choice == "5":
            print("Exiting shopping cart management.")
            break
        else:
            print("Invalid option. Please try again.")

def manage_user_account():
    print("\n--- User Account ---")
    user_id = input("User ID: ")
    response = requests.get(f"{API_URL}/users/{user_id}")
    if response.status_code == 200:
        user_info = response.json()
        print(f"Username: {user_info['username']}, Email: {user_info['email']}")
    else:
        print("Failed to retrieve user account info:", response.json())

#CHIENSEX
def leave_reviews_and_ratings():
    op = input("1. Add Feedback\n2. See Rating\n3, Relation (similars)\n")
    
    # Search for a product
    query = input("Enter product name or keyword: ")
    response = requests.get(f"{API_URL}/search", params={"q": query})
    
    if response.status_code == 200:
        products = response.json()
        if not products:
            print("No products found for your query.")
            return

        print("\n--- Available Products ---")
        for i, product in enumerate(products, start=1):
            print(f"{i}. Name: {product['name']}, Price: ${product['price']})")

        try:
            product_choice = int(input("\nSelect a product by number: "))
            if not (1 <= product_choice <= len(products)):
                print("Invalid selection. Please try again.")
                return
        except ValueError:
            print("Invalid input. Please enter a number.")
            return

        selected_product = products[product_choice - 1]
        product_id = selected_product["_id"]
    if op == '1':
        feedback= input("feedback without comas: ")
        dgraphModel.append_to_csv(product_id, feedback)
    elif op == '2':
        dgraphModel.product_query_rating(product_id)
    elif op == '3':
        dgraphModel.product_query_related(product_id)

def search_products():
    print("\n--- Search Products ---")
    print("Recently searched:" )
    search_bar(user["email"])
    query = input("Enter product name or keyword: ")
    cassandraModel.insert_search_history(session, user["email"], query, "id")
    idList =  []
    response = requests.get(f"{API_URL}/search", params={"q": query})
    if response.status_code == 200:
        products = response.json()
        for product in products:
            idList.append(product['_id'])
            print(f"{product['name']} - ${product['price']} ----- Description: {product['description']} -----")
    else:
        print("Search failed:", response.json())
    cassandraModel.increase_views(session, idList)

def manage_wishlist():
    while True:
        print("\n--- Wishlist Management ---")
        print("1. View Wishlist")
        print("2. Add Product to Wishlist")
        print("3. Remove Product from Wishlist")
        print("4. Exit")

        choice = input("Choose an option: ")

        if choice == "1":
            email = user["email"]
            response = requests.get(f"{API_URL}/viewwishlist/{email}")
            if response.status_code == 200:
                wishlist = response.json()
                if wishlist.get("items"):
                    for item in wishlist["items"]:
                        print(f"{item['name']} - ${item['price']} - Description: {item['description']} - UUID: {item['product_id']}")
                else:
                    print("Your wishlist is empty.")
            else:
                print("Failed to load wishlist:", response.json())

        elif choice == "2":
            email = user["email"]
                # Search for a product
            query = input("Enter product name or keyword: ")
            response = requests.get(f"{API_URL}/search", params={"q": query})
            
            if response.status_code == 200:
                products = response.json()
                if not products:
                    print("No products found for your query.")
                    return

                print("\n--- Available Products ---")
                for i, product in enumerate(products, start=1):
                    print(f"{i}. Name: {product['name']}, Price: ${product['price']})")

                try:
                    product_choice = int(input("\nSelect a product by number: "))
                    if not (1 <= product_choice <= len(products)):
                        print("Invalid selection. Please try again.")
                        return
                except ValueError:
                    print("Invalid input. Please enter a number.")
                    return

                selected_product = products[product_choice - 1]
                product_id = selected_product["_id"]
            else:
                print("Search failed:", response.json())
                return
            
            response = requests.post(f"{API_URL}/addtowishlist/{product_id}/{email}")
            if response.status_code == 201:
                print("Product added to wishlist.")
            else:
                print("Failed to add product:", response.json())

        elif choice == "3":
            email = user["email"]
            product_id = input("Enter Product ID to remove from wishlist: ")
            response = requests.delete(f"{API_URL}/deletefromwishlist/{product_id}/{email}")
            if response.status_code == 200:
                print("Product removed from wishlist.")
            else:
                print("Failed to remove product:", response.json())

        elif choice == "4":
            print("Exiting wishlist management.")
            break
        else:
            print("Invalid option. Please try again.")

def track_orders():
    print("\n--- Order Tracking ---")
    order_id = input("Order ID: ")
    response = requests.get(f"{API_URL}/orders/track/{order_id}")
    if response.status_code == 200:
        tracking_info = response.json()
        print(f"Status: {tracking_info['status']}, Tracking Number: {tracking_info['tracking_number']}")
    else:
        print("Failed to track order:", response.json())

def sell_product():
    print("\n--- Product sell ---")
    name= input("Product name: ")
    desc= input("Product description: ")
    price =float(input("Product price: "))
    catego = input("Product category: ")
    stock= int(input("Product stock: "))
    image= input("Product image url: ")
    response = requests.post(f"{API_URL}/addproduct/{user['email']}", json={"name":name, "description":desc, "price":price, "category":catego, "image":image})
    if response.status_code == 200:
        responseJson=response.json()
        print("Product added:", responseJson)
    else:
        print("Failed to add product:", response.json())

    product_id = responseJson  # Assuming the response only returns the product ID
    total_orders = 0  
    total_revenue = 0.0  
    views = 0
    cassandraModel.insert_product_analytics(session, product_id, total_orders, int(total_revenue), views)
    cassandraModel.insert_inventory(session, product_id, stock)




    

def view_my_products():
    print("\n--- View My Products ---")
    response = requests.get(f"{API_URL}/myproducts/{user['email']}") 
    
    if response.status_code == 200:
        products = response.json()
        if not products:
            print("You have no products listed.")
            return
        
        print("\n--- Your Products ---")
        for product in products:
            print(f"Product ID       : {product['product_id']}")
            print(f"Name             : {product['name']}")
            print(f"Description      : {product['description']}")
            print(f"Price            : ${product['price']:.2f}")
            print(f"Image URL        : {product['image']}")
            print("-" * 30)
    else:
        print(f"Failed to view my products. Status Code: {response.status_code}, Error: {response.text}")


def update_product():
    product_update = {}
    print("\n--- Update product (leave blank to keep current)---")
    idproduct = (input("Product ID: "))
    newname= input("New Name :" )
    newdescription= input("New Description :" )
    newprice = (input("New Price: ")) 
    newimage = input("New Image URL: ")
    newstock = ("New Stock: ")
    newcategory = input("New Category: ")

    if newname:
        product_update["name"] = newname
    if newdescription:
        product_update["description"] = newdescription
    if newprice:
        try:
            product_update["price"] = float(newprice)
        except ValueError: print("Product price is not a number")
    if newcategory:
        product_update["category"] = newcategory 
    if newimage:
        product_update["image"] = newimage
    if newstock:
        pass
    response = requests.put(f"{API_URL}/editproduct/{idproduct}", json=product_update)
    if response.status_code == 200:
        print("Success")
    else:
        print("Error: " , response.content)


def delete_product():
    print("\n--- Delete Product ---")
    idproduct = input("Product ID: ")
    deleted = requests.delete(f"{API_URL}/deleteproduct/{idproduct}?email={user['email']}")
    if deleted.status_code == 200:
        print("Product deleted successfully.")
    else:
        print(f"Failed to delete product. Error: {deleted.status_code} - {deleted.text}")



def logout():
    print("You have been logged out.")


def menu():
    log.info("Connecting to Cluster")
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()

    cassandraModel.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)

    cassandraModel.create_schema(session)
    menu1flag = True
    menu2flag = False
    while(menu1flag):
        print("\n--- Menu ---")
        (print("1. Login\n2. Create account\n3. Exit"))
        option= int(input("Select an option: "))
        if option ==1:
            userlog=user_login()
            if userlog: 
                print(userlog)
                menu2flag = True
                menu1flag = False
        elif option == 2:
            userlog=user_registration()
            if userlog: 
                print(userlog)
                menu2flag = True
                menu1flag = False

        elif option == 3:
            exit(0)
        else:
            print("Error: Unknown option")
    while (menu2flag):
        main_menu()

if __name__ == "__main__":
    menu()
