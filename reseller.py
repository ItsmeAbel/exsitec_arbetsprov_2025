import requests

# Base URL of your local API
BASE_URL = "http://127.0.0.1:8000"

def get_products():
    try:
        response = requests.get(f"{BASE_URL}/products")
        response.raise_for_status()  # raises exception for 4xx/5xx
        products = response.json()
        print("✅ Products received from API:")
        for p in products:
            print(f"- ID: {p['id']}, Name: {p['name']}, Price: {p['price']}, Stock: {p['stock']}")
    except requests.exceptions.RequestException as e:
        print("❌ Error calling API:", e)

if __name__ == "__main__":
    get_products()
