import sqlite3
import pandas as pd
import random
from datetime import datetime, timedelta
import os

def create_ski_products_database():
    # Set seed for reproducible data
    random.seed(20250929)
    
    # Create data directory if it doesn't exist
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    os.makedirs(data_dir, exist_ok=True)
    
    # Database file path
    db_path = os.path.join(data_dir, 'ski_products.db')
    
    # Remove existing database
    if os.path.exists(db_path):
        os.remove(db_path)
    
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create normalized tables
    
    # 1. Categories table
    cursor.execute('''
        CREATE TABLE categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            description TEXT
        )
    ''')
    
    # 2. Brands table
    cursor.execute('''
        CREATE TABLE brands (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            country_of_origin TEXT,
            founded_year INTEGER
        )
    ''')
    
    # 3. Materials table
    cursor.execute('''
        CREATE TABLE materials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            properties TEXT
        )
    ''')
    
    # 4. Products table (main table)
    cursor.execute('''
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category_id INTEGER NOT NULL,
            brand_id INTEGER NOT NULL,
            model TEXT NOT NULL,
            gender TEXT CHECK(gender IN ('Men', 'Women', 'Unisex', 'Kids')),
            skill_level TEXT CHECK(skill_level IN ('Beginner', 'Intermediate', 'Advanced', 'Expert')),
            color TEXT,
            size TEXT,
            length_cm INTEGER,
            weight_kg REAL,
            price REAL NOT NULL CHECK(price > 0),
            discount_percent INTEGER DEFAULT 0 CHECK(discount_percent >= 0 AND discount_percent <= 100),
            material_id INTEGER,
            release_year INTEGER,
            season TEXT,
            warranty_months INTEGER DEFAULT 12,
            sku TEXT UNIQUE NOT NULL,
            barcode TEXT UNIQUE,
            is_active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (category_id) REFERENCES categories (id),
            FOREIGN KEY (brand_id) REFERENCES brands (id),
            FOREIGN KEY (material_id) REFERENCES materials (id)
        )
    ''')
    
    # 5. Inventory table
    cursor.execute('''
        CREATE TABLE inventory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            stock_quantity INTEGER NOT NULL DEFAULT 0,
            reserved_quantity INTEGER DEFAULT 0,
            reorder_point INTEGER DEFAULT 10,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (product_id) REFERENCES products (id)
        )
    ''')
    
    # 6. Product ratings table
    cursor.execute('''
        CREATE TABLE product_ratings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            average_rating REAL CHECK(average_rating >= 1.0 AND average_rating <= 5.0),
            total_reviews INTEGER DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (product_id) REFERENCES products (id)
        )
    ''')
    
    # 7. Tags table
    cursor.execute('''
        CREATE TABLE tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        )
    ''')
    
    # 8. Product tags junction table (many-to-many)
    cursor.execute('''
        CREATE TABLE product_tags (
            product_id INTEGER,
            tag_id INTEGER,
            PRIMARY KEY (product_id, tag_id),
            FOREIGN KEY (product_id) REFERENCES products (id),
            FOREIGN KEY (tag_id) REFERENCES tags (id)
        )
    ''')
    
    # Create indexes for better performance
    cursor.execute('CREATE INDEX idx_products_category ON products(category_id)')
    cursor.execute('CREATE INDEX idx_products_brand ON products(brand_id)')
    cursor.execute('CREATE INDEX idx_products_price ON products(price)')
    cursor.execute('CREATE INDEX idx_products_active ON products(is_active)')
    cursor.execute('CREATE INDEX idx_inventory_stock ON inventory(stock_quantity)')
    
    # Insert reference data
    categories_data = [
        ('Skis', 'Alpine and touring skis for all skill levels'),
        ('Ski Boots', 'Ski boots for alpine and backcountry skiing'),
        ('Ski Poles', 'Adjustable and fixed ski poles'),
        ('Ski Goggles', 'Eye protection for skiing'),
        ('Ski Helmet', 'Head protection for skiing'),
        ('Ski Gloves', 'Hand protection and warmth'),
        ('Ski Jacket', 'Outer layer protection'),
        ('Ski Pants', 'Lower body protection'),
        ('Base Layer', 'Moisture-wicking underwear'),
        ('Ski Socks', 'Specialized skiing socks'),
        ('Backpack', 'Ski touring and day packs'),
        ('Ski Wax', 'Ski maintenance products'),
        ('Bindings', 'Ski binding systems'),
        ('Avalanche Beacon', 'Safety equipment for backcountry'),
        ('Ski Bag', 'Storage and transport')
    ]
    
    brands_data = [
        ('Salomon', 'France', 1947),
        ('Atomic', 'Austria', 1955),
        ('Rossignol', 'France', 1907),
        ('Head', 'Austria', 1950),
        ('Fischer', 'Austria', 1924),
        ('Nordica', 'Italy', 1939),
        ('K2', 'USA', 1962),
        ('Völkl', 'Germany', 1923),
        ('Blizzard', 'Austria', 1945),
        ('Dynastar', 'France', 1963),
        ('Elan', 'Slovenia', 1945),
        ('Black Crows', 'France', 2006),
        ('Scarpa', 'Italy', 1938),
        ('Tecnica', 'Italy', 1960),
        ('Dalbello', 'Italy', 1974),
        ('POC', 'Sweden', 2005),
        ('Giro', 'USA', 1985),
        ('Smith', 'USA', 1965),
        ('Oakley', 'USA', 1975),
        ('Scott', 'Switzerland', 1958)
    ]
    
    materials_data = [
        ('Composite', 'Lightweight composite materials'),
        ('Carbon', 'Carbon fiber construction'),
        ('Aluminum', 'Aluminum alloy construction'),
        ('Polycarbonate', 'Impact-resistant plastic'),
        ('ABS', 'Durable thermoplastic'),
        ('Gore-Tex', 'Waterproof breathable membrane'),
        ('Down', 'Natural insulation'),
        ('Softshell', 'Flexible weather protection'),
        ('Nylon', 'Durable synthetic fabric'),
        ('Merino Wool', 'Natural wool fiber'),
        ('Polyester', 'Synthetic fabric')
    ]
    
    # Insert reference data
    cursor.executemany('INSERT INTO categories (name, description) VALUES (?, ?)', categories_data)
    cursor.executemany('INSERT INTO brands (name, country_of_origin, founded_year) VALUES (?, ?, ?)', brands_data)
    cursor.executemany('INSERT INTO materials (name, properties) VALUES (?, ?)', materials_data)
    
    # Get reference data for foreign keys
    categories_dict = {row[1]: row[0] for row in cursor.execute('SELECT id, name FROM categories').fetchall()}
    brands_dict = {row[1]: row[0] for row in cursor.execute('SELECT id, name FROM brands').fetchall()}
    materials_dict = {row[1]: row[0] for row in cursor.execute('SELECT id, name FROM materials').fetchall()}
    
    # Generate and insert products
    models = ['Pro', 'Elite', 'Carbon', 'X', 'X2', 'XR', 'XT', 'Ultra', 'Tour', 'Racer', 'Vantage', 'All-Mountain', 'Backcountry', 'Carver', 'Speed', 'Edge', 'Prime', 'Peak', 'Vector', 'Nova', 'Legend', 'Storm']
    levels = ['Beginner', 'Intermediate', 'Advanced', 'Expert']
    genders = ['Men', 'Women', 'Unisex', 'Kids']
    colors = ['Black', 'White', 'Red', 'Blue', 'Green', 'Yellow', 'Orange', 'Grey', 'Navy', 'Teal', 'Burgundy']
    
    # Price ranges by category
    price_ranges = {
        'Skis': (299, 1099), 'Ski Boots': (149, 699), 'Ski Poles': (19, 199),
        'Ski Goggles': (39, 299), 'Ski Helmet': (49, 349), 'Ski Gloves': (19, 179),
        'Ski Jacket': (99, 799), 'Ski Pants': (79, 599), 'Base Layer': (19, 149),
        'Ski Socks': (5, 39), 'Backpack': (39, 249), 'Ski Wax': (9, 59),
        'Bindings': (89, 399), 'Avalanche Beacon': (199, 599), 'Ski Bag': (29, 199)
    }
    
    # Insert products
    for product_id in range(1001, 1501):  # 500 products
        category_name = random.choice(list(categories_dict.keys()))
        brand_name = random.choice(list(brands_dict.keys()))
        material_name = random.choice(list(materials_dict.keys()))
        
        category_id = categories_dict[category_name]
        brand_id = brands_dict[brand_name]
        material_id = materials_dict[material_name]
        
        model = random.choice(models)
        gender = random.choice(genders)
        level = random.choice(levels)
        color = random.choice(colors)
        
        product_name = f"{brand_name} {model} {category_name}"
        
        # Generate category-specific attributes
        min_price, max_price = price_ranges.get(category_name, (19, 199))
        price = round(random.uniform(min_price, max_price), 2)
        
        # Size logic
        size = ''
        if category_name == 'Ski Boots':
            size = f"EU {random.randint(36, 48)}"
        elif category_name in ['Ski Helmet', 'Ski Gloves']:
            size = random.choice(['XS', 'S', 'M', 'L', 'XL'])
        elif category_name in ['Ski Jacket', 'Ski Pants', 'Base Layer']:
            size = random.choice(['XS', 'S', 'M', 'L', 'XL', 'XXL'])
        elif category_name == 'Ski Socks':
            size = random.choice(['S', 'M', 'L'])
        
        # Length for skis and poles
        length_cm = None
        if category_name == 'Skis':
            length_cm = random.randint(140, 190)
        elif category_name == 'Ski Poles':
            length_cm = random.randint(90, 140)
        
        # Weight ranges
        weight_ranges = {
            'Skis': (3.0, 7.0), 'Ski Boots': (1.8, 5.0), 'Ski Poles': (0.3, 0.8),
            'Ski Helmet': (0.4, 0.9), 'Ski Jacket': (0.6, 1.6), 'Ski Pants': (0.5, 1.4),
            'Backpack': (0.6, 1.8), 'Bindings': (1.0, 2.8), 'Avalanche Beacon': (0.2, 0.5)
        }
        min_weight, max_weight = weight_ranges.get(category_name, (0.1, 0.9))
        weight_kg = round(random.uniform(min_weight, max_weight), 2)
        
        # Generate other attributes
        discount_percent = random.choices([0, 5, 10, 15, 20, 25, 30], weights=[40, 10, 20, 10, 10, 5, 5])[0]
        release_year = random.randint(2019, 2025)
        season = f"AW{random.randint(2021, 2025)}"
        warranty_months = random.choice([12, 24, 24, 24, 36])
        
        # Generate SKU and barcode
        brand_code = ''.join([c for c in brand_name.upper() if c.isalpha()])[:3].ljust(3, 'X')
        category_code = ''.join([c for c in category_name.upper() if c.isalpha()])[:3].ljust(3, 'X')
        sku = f"SKU-{brand_code}-{category_code}-{product_id}"
        barcode = ''.join([str(random.randint(0, 9)) for _ in range(13)])
        
        is_active = random.choices([1, 0], weights=[95, 5])[0]
        
        # Insert product
        cursor.execute('''
            INSERT INTO products (
                id, name, category_id, brand_id, model, gender, skill_level, color, size,
                length_cm, weight_kg, price, discount_percent, material_id, release_year,
                season, warranty_months, sku, barcode, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            product_id, product_name, category_id, brand_id, model, gender, level, color, size,
            length_cm, weight_kg, price, discount_percent, material_id, release_year,
            season, warranty_months, sku, barcode, is_active
        ))
        
        # Insert inventory
        stock_quantity = random.randint(0, 250)
        reserved_quantity = random.randint(0, min(stock_quantity, 20))
        reorder_point = random.randint(5, 50)
        
        cursor.execute('''
            INSERT INTO inventory (product_id, stock_quantity, reserved_quantity, reorder_point)
            VALUES (?, ?, ?, ?)
        ''', (product_id, stock_quantity, reserved_quantity, reorder_point))
        
        # Insert rating
        rating = round(3.0 + (2.0 * random.random()), 1)
        total_reviews = random.randint(0, 150)
        
        cursor.execute('''
            INSERT INTO product_ratings (product_id, average_rating, total_reviews)
            VALUES (?, ?, ?)
        ''', (product_id, rating, total_reviews))
    
    # Insert tags and product-tag relationships
    all_tags = set()
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
    
    base_tags = ['skiing', 'winter', 'outdoor']
    all_tags.update(base_tags)
    for tags_list in category_tags.values():
        all_tags.update(tags_list)
    
    # Insert all tags
    for tag in all_tags:
        cursor.execute('INSERT INTO tags (name) VALUES (?)', (tag,))
    
    # Get tag IDs
    tags_dict = {row[1]: row[0] for row in cursor.execute('SELECT id, name FROM tags').fetchall()}
    
    # Add tags to products
    products = cursor.execute('SELECT id, category_id FROM products').fetchall()
    for product_id, category_id in products:
        category_name = next(name for name, id in categories_dict.items() if id == category_id)
        
        # Add base tags
        for tag in base_tags:
            cursor.execute('INSERT INTO product_tags (product_id, tag_id) VALUES (?, ?)',
                         (product_id, tags_dict[tag]))
        
        # Add category-specific tags
        if category_name in category_tags:
            specific_tags = random.sample(category_tags[category_name], 
                                        min(random.randint(1, 3), len(category_tags[category_name])))
            for tag in specific_tags:
                cursor.execute('INSERT INTO product_tags (product_id, tag_id) VALUES (?, ?)',
                             (product_id, tags_dict[tag]))
    
    # Commit all changes
    conn.commit()
    
    print(f"Database created successfully: {db_path}")
    print(f"Tables created: categories, brands, materials, products, inventory, product_ratings, tags, product_tags")
    print(f"Generated 500 products with normalized relational structure")
    
    # Show some example queries
    print("\n--- Example Queries ---")
    
    # Count products by category
    print("\nProducts by category:")
    cursor.execute('''
        SELECT c.name, COUNT(p.id) as product_count
        FROM categories c
        LEFT JOIN products p ON c.id = p.category_id
        GROUP BY c.name
        ORDER BY product_count DESC
    ''')
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]} products")
    
    # Average price by brand
    print("\nTop 5 brands by average price:")
    cursor.execute('''
        SELECT b.name, ROUND(AVG(p.price), 2) as avg_price
        FROM brands b
        JOIN products p ON b.id = p.brand_id
        GROUP BY b.name
        ORDER BY avg_price DESC
        LIMIT 5
    ''')
    for row in cursor.fetchall():
        print(f"  {row[0]}: ${row[1]}")
    
    conn.close()
    return db_path

if __name__ == "__main__":
    create_ski_products_database()