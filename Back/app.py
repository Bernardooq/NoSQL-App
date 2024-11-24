import requests

API_URL = "http://127.0.0.1:8000" 

def main_menu():
    while True:
        print("\n--- Welcome to the Store App ---")
        print("1. Search history")
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
        print("12. Admin Dashboard")
        print("13. Logout")
        print("0. Exit")
        
        choice = input("Select an option: ")

        if choice == "1":
            search_history()
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
            admin_dashboard()
        elif choice == "13":
            logout()
        elif choice == "0":
            print("Exiting... Goodbye!")
            break
        else:
            print("Invalid choice. Please try again.")

def search_history(var = 0):
    print(f"Searching history page {var+1}...")
    myvar = var
    response= requests.get(f"{API_URL}/history/{range}")
    if response.status_code == 200:
        history = response.json()
        for i in range(myvar): 
            print(history[i])
    print("Next page (type 1) \nPrevious page (type 2)\n Exit (type enter)")
    page= int(input("Option: "))
    if page == 1:
        myvar+=10
        search_history(myvar)
    elif page == 2:
        if myvar!=0: 
            myvar-10 
            search_history(myvar)
        else: 
            print("You are at page 1")
            search_history(myvar)

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
    
    if response.status_code == 201:
        print("Registration successful!")
        return response.json()
    else:
        print("Registration failed:", response.json())
        return ''

def user_login():
    print("\n--- User Login ---")
    email = input("Email: ")
    password = input("Password: ")
    response = requests.post(f"{API_URL}/login", json = {"email": email, "password": password})

    if response.status_code == 200:
        print("Login successful")
        return response.json()
    else:
        print("Login failed: ", response.json())
        return ''


def update_profile():
    print("\n--- Update Profile ---")
    user_id = input("User ID: ")
    new_email = input("New Email (leave blank to keep current): ")
    new_password = input("New Password (leave blank to keep current): ")

    payload = {}
    if new_email: payload["email"] = new_email
    if new_password: payload["password"] = new_password

    response = requests.put(f"{API_URL}/users/{user_id}", json=payload)
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

def admin_dashboard():
    print("\n--- Admin Dashboard ---")
    print("Coming soon!")

def logout():
    print("You have been logged out.")

def menu():
    menu1flag = True
    menu2flag = False
    data= ''
    while(menu1flag):
        print("\n--- Menu ---")
        (print("1. Login\n2. Create account"))
        option= int(input("Select an option: "))
        if option ==1:
            data= user_login()  
            if data != '': 
                menu2flag = True
                menu1flag = False
        elif option == 2:
            data = user_registration()
            if data != '': 
                menu2flag = True
                menu1flag = False
    while (menu2flag):
        main_menu()

if __name__ == "__main__":
    menu()
