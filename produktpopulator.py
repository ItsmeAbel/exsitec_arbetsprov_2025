import pandas as pd
import random
from datetime import datetime, timedelta
import os

def generate_ski_products_excel():
    # Set seed for reproducible data
    random.seed(20250929)
    
    # Product data arrays
    categories = [
        'Skis', 'Ski Boots', 'Ski Poles', 'Ski Goggles', 'Ski Helmet', 
        'Ski Gloves', 'Ski Jacket', 'Ski Pants', 'Base Layer', 'Ski Socks',
        'Backpack', 'Ski Wax', 'Bindings', 'Avalanche Beacon', 'Ski Bag'
    ]
    
    brands = [
        'Salomon', 'Atomic', 'Rossignal', 'Head', 'Fischer', 'Nordica',
        'K2', 'Völkl', 'Blizzard', 'Dynastar', 'Elan', 'Black Crows',
        'Scarpa', 'Tecnica', 'Dalbello', 'POC', 'Giro', 'Smith', 'Oakley', 'Scott'
    ]
    
    models = [
        'Pro', 'Elite', 'Carbon', 'X', 'X2', 'XR', 'XT', 'Ultra', 'Tour',
        'Racer', 'Vantage', 'All-Mountain', 'Backcountry', 'Carver', 'Speed',
        'Edge', 'Prime', 'Peak', 'Vector', 'Nova', 'Legend', 'Storm'
    ]
    
    levels = ['Beginner', 'Intermediate', 'Advanced', 'Expert']
    genders = ['Men', 'Women', 'Unisex', 'Kids']
    colors = ['Black', 'White', 'Red', 'Blue', 'Green', 'Yellow', 'Orange', 'Grey', 'Navy', 'Teal', 'Burgundy']
    materials = ['Composite', 'Carbon', 'Aluminum', 'Polycarbonate', 'ABS', 'Gore-Tex', 'Down', 'Softshell', 'Nylon', 'Merino Wool', 'Polyester']
    countries = ['Austria', 'Germany', 'Italy', 'France', 'Slovenia', 'Czechia', 'Poland', 'Romania', 'China', 'Vietnam']
    
    def get_price(category):
        price_ranges = {
            'Skis': (299, 1099),
            'Ski Boots': (149, 699),
            'Ski Poles': (19, 199),
            'Ski Goggles': (39, 299),
            'Ski Helmet': (49, 349),
            'Ski Gloves': (19, 179),
            'Ski Jacket': (99, 799),
            'Ski Pants': (79, 599),
            'Base Layer': (19, 149),
            'Ski Socks': (5, 39),
            'Backpack': (39, 249),
            'Ski Wax': (9, 59),
            'Bindings': (89, 399),
            'Avalanche Beacon': (199, 599),
            'Ski Bag': (29, 199)
        }
        min_price, max_price = price_ranges.get(category, (19, 199))
        return round(random.uniform(min_price, max_price), 2)
    
    def get_weight(category):
        weight_ranges = {
            'Skis': (3.0, 7.0),
            'Ski Boots': (1.8, 5.0),
            'Ski Poles': (0.3, 0.8),
            'Ski Helmet': (0.4, 0.9),
            'Ski Jacket': (0.6, 1.6),
            'Ski Pants': (0.5, 1.4),
            'Backpack': (0.6, 1.8),
            'Bindings': (1.0, 2.8),
            'Avalanche Beacon': (0.2, 0.5)
        }
        min_weight, max_weight = weight_ranges.get(category, (0.1, 0.9))
        return round(random.uniform(min_weight, max_weight), 2)
    
    def get_size(category, gender):
        if category == 'Ski Boots':
            return f"EU {random.randint(36, 48)}"
        elif category in ['Ski Helmet', 'Ski Gloves']:
            return random.choice(['XS', 'S', 'M', 'L', 'XL'])
        elif category in ['Ski Jacket', 'Ski Pants', 'Base Layer']:
            return random.choice(['XS', 'S', 'M', 'L', 'XL', 'XXL'])
        elif category == 'Ski Socks':
            return random.choice(['S', 'M', 'L'])
        return ''
    
    def get_length(category):
        if category == 'Skis':
            return random.randint(140, 190)
        elif category == 'Ski Poles':
            return random.randint(90, 140)
        return ''
    
    def generate_sku(brand, category, product_id):
        brand_code = ''.join([c for c in brand.upper() if c.isalpha()])[:3].ljust(3, 'X')
        category_code = ''.join([c for c in category.upper() if c.isalpha()])[:3].ljust(3, 'X')
        return f"SKU-{brand_code}-{category_code}-{product_id}"
    
    def generate_barcode():
        return ''.join([str(random.randint(0, 9)) for _ in range(13)])
    
    def get_tags(category):
        base_tags = ['skiing', 'winter', 'outdoor']
        category_tags = {
            'Skis': ['alpine', 'all-mountain', 'carving', 'freeride'],
            'Ski Boots': ['alpine', 'touring'],
            'Ski Poles': ['adjustable', 'fixed'],
            'Ski Goggles': ['mirrored', 'photochromic', 'anti-fog'],
            'Ski Helmet': ['mips', 'lightweight', 'vented'],
            'Ski Gloves': ['insulated', 'leather', 'liner'],
            'Ski Jacket': ['insulated', 'shell', 'gore-tex'],
            'Ski Pants': ['bib', 'shell', 'insulated'],
            'Base Layer': ['merino', 'synthetic'],
            'Ski Socks': ['merino', 'compression'],
            'Backpack': ['avalanche', 'hydration'],
            'Ski Wax': ['cold', 'universal', 'warm'],
            'Bindings': ['gripwalk', 'touring'],
            'Avalanche Beacon': ['rescue', 'safety'],
            'Ski Bag': ['roller', 'padded']
        }
        specific_tags = category_tags.get(category, [])
        selected_tags = base_tags + random.sample(specific_tags, min(random.randint(1, 3), len(specific_tags)))
        return '; '.join(list(set(selected_tags)))
    
    # Generate data
    products = []
    for product_id in range(1001, 1501):  # 500 products (1001-1500)
        category = random.choice(categories)
        brand = random.choice(brands)
        model = random.choice(models)
        gender = random.choice(genders)
        level = random.choice(levels)
        color = random.choice(colors)
        
        product_name = f"{brand} {model} {category}"
        price = get_price(category)
        stock = random.randint(0, 250)
        weight = get_weight(category)
        size = get_size(category, gender)
        length = get_length(category)
        release_year = random.randint(2019, 2025)
        season = f"AW{random.randint(2021, 2025)}"
        sku = generate_sku(brand, category, product_id)
        barcode = generate_barcode()
        rating = round(3.0 + (2.0 * random.random()), 1)
        active = random.choices([True, False], weights=[95, 5])[0]
        discount_percent = random.choices([0, 5, 10, 15, 20, 25, 30], weights=[40, 10, 20, 10, 10, 5, 5])[0]
        
        # Generate timestamps
        created_days_ago = random.randint(0, 900)
        updated_days_ago = random.randint(0, 90)
        created_at = (datetime.now() - timedelta(days=created_days_ago)).strftime('%Y-%m-%d %H:%M:%S')
        updated_at = (datetime.now() - timedelta(days=updated_days_ago)).strftime('%Y-%m-%d %H:%M:%S')
        
        product = {
            'Id': product_id,
            'ProductName': product_name,
            'Category': category,
            'Brand': brand,
            'Model': model,
            'Gender': gender,
            'Level': level,
            'Color': color,
            'Size': size,
            'LengthCm': length if length else None,
            'Price': price,
            'Stock': stock,
            'DiscountPercent': discount_percent,
            'Rating': rating,
            'WeightKg': weight,
            'ReleaseYear': release_year,
            'Season': season,
            'WarrantyMonths': random.choice([12, 24, 24, 24, 36]),
            'Material': random.choice(materials),
            'CountryOfOrigin': random.choice(countries),
            'SKU': sku,
            'Barcode': barcode,
            'Active': active,
            'Tags': get_tags(category),
            'CreatedAt': created_at,
            'UpdatedAt': updated_at
        }
        
        products.append(product)
    
    # Create DataFrame
    df = pd.DataFrame(products)
    
    # Create data directory if it doesn't exist
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    os.makedirs(data_dir, exist_ok=True)
    
    # Save to Excel file
    excel_file = os.path.join(data_dir, 'ski_products.xlsx')
    with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Ski Products', index=False)
        
        # Auto-adjust column widths
        worksheet = writer.sheets['Ski Products']
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)  # Cap at 50 characters
            worksheet.column_dimensions[column_letter].width = adjusted_width
    
    print(f"Generated Excel file with {len(products)} skiing products: {excel_file}")
    return excel_file

if __name__ == "__main__":
    generate_ski_products_excel()