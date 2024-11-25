import requests

API_URL = "http://127.0.0.1:8000" 
global user
user = {}

def main_menu():
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
        print("   5.3. Update Product")
        print("   5.4. Delete Product")
        print("6. Reviews and Ratings")
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
            update_product()
        elif choice == "5.4":
            delete_product()

        # Reviews and Ratings
        elif choice == "6":
            leave_reviews_and_ratings()

        # Exit
        elif choice == "0":
            print("Exiting... Goodbye!")
            break

        else:
            print("Invalid choice. Please try again.")

def see_profile():
    print("\n--- See Profile ---")
    response = requests.get(f"{API_URL}/profiledetails/{user['email']}")  # Usa el email del usuario autenticado
    if response.status_code == 200:
        profile = response.json()
        print("Profile Details:")
        print(f"Id: {profile['_id']}")
        print(f"Email: {profile['email']}")
        print(f"Username: {profile['username']}")
        print(f"Address: {profile['address']}")  
        print(f"Registration Date: {profile['created_at']}")
    else:
        print("Failed to fetch profile details:", response.json())

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
    response = requests.post(f"{API_URL}/login", json = {"email": email, "password": password})

    if response.status_code == 200:
        response_data= response.json()
        user["email"]=response_data["email"]
        print("Login successful")
        return response.json()
    else:
        print("Login failed:", response)
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

import requests

API_URL = "http://localhost:8000"  # Cambia la URL según corresponda

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
    user_id = input("User ID: ")
    product_id = input("Product ID: ")
    quantity = int(input("Quantity: "))
    payment_method = input("Payment Method (Card/Paypal): ")

    response = requests.post(f"{API_URL}/orders", json={
        "user_id": user_id,
        "product_id": product_id,
        "quantity": quantity,
        "payment_method": payment_method
    })

    if response.status_code == 201:
        print("Order created successfully!")
    else:
        print("Failed to create order:", response.json())

def view_purchase_history():
    print("\n--- Purchase History ---")
    user_id = input("User ID: ")
    response = requests.get(f"{API_URL}/orders/{user_id}")
    if response.status_code == 200:
        orders = response.json()
        for order in orders:
            print(f"Order ID: {order['order_id']}, Total: ${order['total']}, Status: {order['status']}")
    else:
        print("Failed to retrieve purchase history:", response.json())

def manage_shopping_cart():
    while True:
        print("\n--- Shopping Cart ---")
        email = user["email"]
        response = requests.get(f"{API_URL}/cart/{email}")
        if response.status_code == 200:
            cart = response.json()
            if cart["items"]:
                print("\nYour Cart:")
                for i, item in enumerate(cart["items"], start=1):
                    print(f"{i}. {item['name']} x{item['quantity']} - ${item['price']} - UUID: {item['product_id']}")
            else:
                print("\nYour cart is empty.")
        else:
            print("Failed to retrieve cart:", response.json())
            break

        print("\n--- Menu ---")
        print("1. Add product to cart")
        print("2. Remove product from cart")
        print("3. Update product quantity in cart")
        print("4. Exit")

        choice = input("Choose an option: ")
        if choice == "1":
            pid= input("Product ID: ")
            newresponse = requests.post(f"{API_URL}/addtocart/{pid}/{email}")
        elif choice == "2":
            pid= input("Product ID: ")
            newresponse = requests.delete(f"{API_URL}/deletefromcart/{pid}/{email}")
        elif choice == "3":
            pid= input("Product ID: ")
            newamount= int(input("New amount: "))
            newresponse = requests.put(f"{API_URL}/editcart/{pid}/{newamount}/{email}")
        elif choice == "4":
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

def leave_reviews_and_ratings():
    print("\n--- Leave Reviews and Ratings ---")
    user_id = input("User ID: ")
    product_id = input("Product ID: ")
    rating = int(input("Rating (1-5): "))
    review_text = input("Review: ")

    response = requests.post(f"{API_URL}/reviews", json={
        "user_id": user_id,
        "product_id": product_id,
        "rating": rating,
        "review_text": review_text
    })

    if response.status_code == 201:
        print("Review submitted successfully!")
    else:
        print("Failed to submit review:", response.json())

def search_products():
    print("\n--- Search Products ---")
    query = input("Enter product name or keyword: ")
    response = requests.get(f"{API_URL}/search", params={"q": query})
    if response.status_code == 200:
        products = response.json()
        for product in products:
            print(f"{product['name']} - ${product['price']} ----- Description: {product['description']} -----")
    else:
        print("Search failed:", response.json())

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
            product_id = input("Enter Product ID to add to wishlist: ")
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

def view_my_products():
    print("\n--- View my products ---")
    response = requests.get(f"{API_URL}/myproducts/{user['email']}") 
    if response.status_code == 200:
        print(response.json())
    else:
        print("Failed to view my products:", response)

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

