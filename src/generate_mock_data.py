import os
import random
import csv
from datetime import datetime, timedelta

def generate_mock_data(output_path="data/raw/events.csv", num_rows=10000):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    event_types = ["view", "cart", "purchase"]
    brands = ["apple", "samsung", "xiaomi", "huawei", "oppo", None]
    
    # Pre-define some products to simulate price changes (SCD2)
    products = {
        101: {"category_id": 1, "brand": "apple", "base_price": 999.0},
        102: {"category_id": 1, "brand": "samsung", "base_price": 899.0},
        103: {"category_id": 2, "brand": "xiaomi", "base_price": 299.0},
        104: {"category_id": 2, "brand": "huawei", "base_price": 499.0},
        105: {"category_id": 3, "brand": None, "base_price": 49.0},
    }
    
    start_time = datetime(2024, 1, 1, 0, 0, 0)
    
    headers = [
        "event_time",
        "event_type",
        "product_id",
        "category_id",
        "category_code",
        "brand",
        "price",
        "user_id",
        "user_session"
    ]
    
    print(f"Generating {num_rows} mock rows to {output_path}...")
    
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        
        for i in range(num_rows):
            # Advance time slightly for each row
            event_time = start_time + timedelta(seconds=random.randint(10, 300) * i)
            
            # Select product
            p_id = random.choice(list(products.keys()))
            p_info = products[p_id]
            
            # Simulate a price change at some point
            price = p_info["base_price"]
            if event_time > datetime(2024, 1, 15) and p_id == 101:
                price = 1049.0  # Price goes up
            elif event_time > datetime(2024, 2, 1) and p_id == 101:
                price = 949.0   # Price goes down
                
            category_id = p_info["category_id"]
            brand = p_info["brand"]
            category_code = f"electronics.smartphone" if category_id == 1 else "electronics.audio"
            
            # Random event type, user_id, and user_session
            event_type = random.choice(event_types)
            user_id = random.randint(1000, 2000)
            user_session = f"session_{random.randint(1, 500)}"
            
            writer.writerow([
                event_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                event_type,
                p_id,
                category_id,
                category_code,
                brand if brand else "",
                price,
                user_id,
                user_session
            ])
            
    print("Generation complete!")

if __name__ == "__main__":
    generate_mock_data()
