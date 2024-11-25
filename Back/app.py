import requests

API_URL = "http://127.0.0.1:8000" 
global user
user = {}

def main_menu():
    while True:
        print("\n--- Welcome to the Store App ---")
        print("1. See profile")
        print("2. Update Profile")
        print("3. Product Catalog")
        print("4. Create Purchase Order")
        print("5. View Purchase History")
        print("6. Shopping Cart")
        print("7. User Account")
        print("8. Reviews and Ratings")
        print("9. Search Products")
        print("10. Wishlist")
        print("11. Order Tracking")
        print("12. Sell New Products")
        print("13. My Products")
        print("14. Update product")
        print("15. Delete My Products")
        print("0. Exit")
        
        choice = input("Select an option: ")

        if choice == "1":
            see_profile()
        elif choice == "2":
            update_profile()
        elif choice == "3":
            view_product_catalog()
        elif choice == "4":
            create_purchase_order()
        elif choice == "5":
            view_purchase_history()
        elif choice == "6":
            manage_shopping_cart()
        elif choice == "7":
            manage_user_account()
        elif choice == "8":
            leave_reviews_and_ratings()
        elif choice == "9":
            search_products()
        elif choice == "10":
            manage_wishlist()
        elif choice == "11":
            track_orders()
        elif choice == "12":
            sell_product()
        elif choice == "13":
            view_my_products()
        elif choice == "14":
            update_product()
        elif choice == "15":
            delete_product()
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

def view_product_catalog():
    print("\n--- Product Catalog ---")
    response = requests.get(f"{API_URL}/products")
    if response.status_code == 200:
        products = response.json()
        for product in products:
            print(f"{product['name']} - ${product['price']} (Stock: {product['stock']})")
    else:
        print("Failed to load product catalog:", response.json())

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
    print("\n--- Shopping Cart ---")
    user_id = input("User ID: ")
    response = requests.get(f"{API_URL}/cart/{user_id}")
    if response.status_code == 200:
        cart = response.json()
        for item in cart['items']:
            print(f"{item['product_name']} x{item['quantity']} - ${item['price']}")
        print("Total:", cart['total'])
    else:
        print("Failed to retrieve cart:", response.json())

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
            print(f"{product['name']} - ${product['price']} (Stock: {product['stock']})")
    else:
        print("Search failed:", response.json())

def manage_wishlist():
    print("\n--- Wishlist ---")
    user_id = input("User ID: ")
    response = requests.get(f"{API_URL}/wishlist/{user_id}")
    if response.status_code == 200:
        wishlist = response.json()
        for item in wishlist:
            print(f"{item['product_name']} added on {item['date_added']}")
    else:
        print("Failed to load wishlist:", response.json())

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
        print("Success")
    else: print("Failed to delete product")

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

