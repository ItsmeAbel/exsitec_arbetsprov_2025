import argparse
import os
import sys
import time
import sqlite3
from typing import Dict, Optional, Tuple, Iterable

import pandas as pd

REQUIRED_COLS = ["Id", "ProductName", "Price", "Stock"]
OPTIONAL_COLS = [
    "Category", "Brand", "Model", "Gender", "Level", "Color", "Size", "LengthCm",
    "DiscountPercent", "Rating", "WeightKg", "ReleaseYear", "Season",
    "WarrantyMonths", "Material", "CountryOfOrigin", "SKU", "Barcode", "Active", "Tags",
    "TotalReviews"
]

def load_sheet(path: str) -> pd.DataFrame:
    _, ext = os.path.splitext(path.lower())
    if ext in [".xlsx", ".xls", ".xlsm"]:
        try:
            df = pd.read_excel(path, sheet_name=0, engine="openpyxl")
        except ImportError:
            raise SystemExit("openpyxl is required to read Excel files. Install: pip install openpyxl")
    elif ext == ".csv":
        df = pd.read_csv(path)
    else:
        raise SystemExit(f"Unsupported file extension: {ext}. Use .xlsx or .csv")
    # Normalize columns
    df.columns = [c.strip() for c in df.columns]
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing required columns: {missing}")
    return df

def ensure_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")
    # Dimensions and main tables
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        description TEXT
    );
    CREATE TABLE IF NOT EXISTS brands (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        country_of_origin TEXT,
        founded_year INTEGER
    );
    CREATE TABLE IF NOT EXISTS materials (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        properties TEXT
    );
    CREATE TABLE IF NOT EXISTS tags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        category_id INTEGER NOT NULL,
        brand_id INTEGER NOT NULL,
        model TEXT,
        gender TEXT,
        skill_level TEXT,
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
        sku TEXT UNIQUE,
        barcode TEXT UNIQUE,
        is_active INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (category_id) REFERENCES categories (id),
        FOREIGN KEY (brand_id) REFERENCES brands (id),
        FOREIGN KEY (material_id) REFERENCES materials (id)
    );

    -- Make product_id unique to support UPSERT
    CREATE TABLE IF NOT EXISTS inventory (
        product_id INTEGER PRIMARY KEY,
        stock_quantity INTEGER NOT NULL DEFAULT 0,
        reserved_quantity INTEGER DEFAULT 0,
        reorder_point INTEGER DEFAULT 10,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES products (id)
    );

    CREATE TABLE IF NOT EXISTS product_ratings (
        product_id INTEGER PRIMARY KEY,
        average_rating REAL CHECK(average_rating >= 0.0 AND average_rating <= 5.0),
        total_reviews INTEGER DEFAULT 0,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES products (id)
    );

    CREATE TABLE IF NOT EXISTS product_tags (
        product_id INTEGER NOT NULL,
        tag_id INTEGER NOT NULL,
        PRIMARY KEY (product_id, tag_id),
        FOREIGN KEY (product_id) REFERENCES products (id),
        FOREIGN KEY (tag_id) REFERENCES tags (id)
    );

    CREATE INDEX IF NOT EXISTS idx_products_category ON products(category_id);
    CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand_id);
    CREATE INDEX IF NOT EXISTS idx_products_price ON products(price);
    CREATE INDEX IF NOT EXISTS idx_products_active ON products(is_active);
    """)
    conn.commit()

def get_or_create_dim(conn: sqlite3.Connection, table: str, name: Optional[str]) -> Optional[int]:
    if not name or str(name).strip() == "":
        return None
    name = str(name).strip()
    cur = conn.cursor()
    cur.execute(f"INSERT INTO {table} (name) VALUES (?) ON CONFLICT(name) DO NOTHING;", (name,))
    conn.commit()
    cur.execute(f"SELECT id FROM {table} WHERE name = ?;", (name,))
    row = cur.fetchone()
    return row[0] if row else None

def upsert_product(conn: sqlite3.Connection, row: pd.Series, fk: Dict[str, Optional[int]]):
    cur = conn.cursor()
    # Coerce types/safe defaults
    def get(col, default=None):
        return row[col] if col in row and pd.notna(row[col]) else default

    product_id = int(get("Id"))
    name = str(get("ProductName", f"Product {product_id}"))
    category_id = fk.get("category_id")
    brand_id = fk.get("brand_id")
    material_id = fk.get("material_id")
    model = get("Model")
    gender = get("Gender")
    level = get("Level")
    color = get("Color")
    size = get("Size")
    length_cm = int(get("LengthCm")) if pd.notna(get("LengthCm")) else None
    weight_kg = float(get("WeightKg")) if pd.notna(get("WeightKg")) else None
    price = float(get("Price"))
    discount = int(get("DiscountPercent", 0)) if pd.notna(get("DiscountPercent", 0)) else 0
    release_year = int(get("ReleaseYear")) if pd.notna(get("ReleaseYear")) else None
    season = get("Season")
    warranty_months = int(get("WarrantyMonths", 12)) if pd.notna(get("WarrantyMonths", 12)) else 12
    sku = str(get("SKU")) if pd.notna(get("SKU")) else None
    barcode = str(get("Barcode")) if pd.notna(get("Barcode")) else None
    is_active = get("Active", 1)
    if isinstance(is_active, str):
        is_active = 1 if is_active.strip().lower() in ("1", "true", "yes", "y") else 0
    elif pd.isna(is_active):
        is_active = 1
    else:
        is_active = int(bool(is_active))

    cur.execute("""
        INSERT INTO products (
            id, name, category_id, brand_id, model, gender, skill_level, color, size,
            length_cm, weight_kg, price, discount_percent, material_id, release_year,
            season, warranty_months, sku, barcode, is_active
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name,
            category_id=excluded.category_id,
            brand_id=excluded.brand_id,
            model=excluded.model,
            gender=excluded.gender,
            skill_level=excluded.skill_level,
            color=excluded.color,
            size=excluded.size,
            length_cm=excluded.length_cm,
            weight_kg=excluded.weight_kg,
            price=excluded.price,
            discount_percent=excluded.discount_percent,
            material_id=excluded.material_id,
            release_year=excluded.release_year,
            season=excluded.season,
            warranty_months=excluded.warranty_months,
            sku=excluded.sku,
            barcode=excluded.barcode,
            is_active=excluded.is_active,
            updated_at=CURRENT_TIMESTAMP;
    """, (
        product_id, name, category_id, brand_id, model, gender, level, color, size,
        length_cm, weight_kg, price, discount, material_id, release_year,
        season, warranty_months, sku, barcode, is_active
    ))
    conn.commit()

    # Inventory upsert
    stock_qty = int(get("Stock", 0))
    reserved_qty = 0
    reorder_point = 10
    cur.execute("""
        INSERT INTO inventory (product_id, stock_quantity, reserved_quantity, reorder_point)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(product_id) DO UPDATE SET
          stock_quantity=excluded.stock_quantity,
          reserved_quantity=excluded.reserved_quantity,
          reorder_point=excluded.reorder_point,
          last_updated=CURRENT_TIMESTAMP;
    """, (product_id, stock_qty, reserved_qty, reorder_point))

    # Rating upsert
    avg_rating = float(get("Rating", 0)) if pd.notna(get("Rating", 0)) else 0.0
    total_reviews = int(get("TotalReviews", 0)) if pd.notna(get("TotalReviews", 0)) else 0
    cur.execute("""
        INSERT INTO product_ratings (product_id, average_rating, total_reviews)
        VALUES (?, ?, ?)
        ON CONFLICT(product_id) DO UPDATE SET
          average_rating=excluded.average_rating,
          total_reviews=excluded.total_reviews,
          last_updated=CURRENT_TIMESTAMP;
    """, (product_id, avg_rating, total_reviews))
    conn.commit()

def sync_tags(conn: sqlite3.Connection, product_id: int, tags_value: Optional[str]):
    # Parse tags from "a;b;c" or "a, b, c"
    tags: Iterable[str] = []
    if tags_value and isinstance(tags_value, str):
        if ";" in tags_value:
            parts = [t.strip() for t in tags_value.split(";")]
        else:
            parts = [t.strip() for t in tags_value.split(",")]
        tags = [t for t in parts if t]
    cur = conn.cursor()
    # Insert tags (ignore existing)
    tag_ids = []
    for t in tags:
        cur.execute("INSERT INTO tags (name) VALUES (?) ON CONFLICT(name) DO NOTHING;", (t,))
        cur.execute("SELECT id FROM tags WHERE name = ?;", (t,))
        r = cur.fetchone()
        if r:
            tag_ids.append(r[0])
    # Ensure links
    for tid in tag_ids:
        cur.execute("INSERT OR IGNORE INTO product_tags (product_id, tag_id) VALUES (?, ?);", (product_id, tid))
    # Remove links that are no longer present
    if tag_ids:
        cur.execute(
            f"DELETE FROM product_tags WHERE product_id = ? AND tag_id NOT IN ({','.join(['?']*len(tag_ids))});",
            (product_id, *tag_ids)
        )
    else:
        cur.execute("DELETE FROM product_tags WHERE product_id = ?;", (product_id,))
    conn.commit()

def sync_excel_to_sqlite(excel_path: str, db_path: str, purge_missing: bool = False):
    df = load_sheet(excel_path)
    conn = sqlite3.connect(db_path)
    try:
        ensure_schema(conn)

        # Dimension caches (name -> id)
        cat_cache: Dict[str, int] = {}
        brand_cache: Dict[str, int] = {}
        mat_cache: Dict[str, int] = {}

        # Upsert rows
        seen_product_ids = set()
        for _, r in df.iterrows():
            # Dimensions
            cat_name = str(r["Category"]).strip() if "Category" in r and pd.notna(r["Category"]) else None
            brand_name = str(r["Brand"]).strip() if "Brand" in r and pd.notna(r["Brand"]) else None
            mat_name = str(r["Material"]).strip() if "Material" in r and pd.notna(r["Material"]) else None

            if cat_name not in cat_cache:
                cat_cache[cat_name] = get_or_create_dim(conn, "categories", cat_name) if cat_name else None
            if brand_name not in brand_cache:
                brand_cache[brand_name] = get_or_create_dim(conn, "brands", brand_name) if brand_name else None
            if mat_name not in mat_cache:
                mat_cache[mat_name] = get_or_create_dim(conn, "materials", mat_name) if mat_name else None

            fk = {
                "category_id": cat_cache.get(cat_name),
                "brand_id": brand_cache.get(brand_name),
                "material_id": mat_cache.get(mat_name),
            }

            upsert_product(conn, r, fk)

            pid = int(r["Id"])
            seen_product_ids.add(pid)

            # Tags
            tags_val = r["Tags"] if "Tags" in r else None
            sync_tags(conn, pid, tags_val)

        # Optionally mark products missing from Excel as inactive
        if purge_missing:
            cur = conn.cursor()
            cur.execute("SELECT id FROM products;")
            existing_ids = {row[0] for row in cur.fetchall()}
            missing_ids = list(existing_ids - seen_product_ids)
            if missing_ids:
                qmarks = ",".join(["?"] * len(missing_ids))
                cur.execute(f"UPDATE products SET is_active = 0, updated_at=CURRENT_TIMESTAMP WHERE id IN ({qmarks});", missing_ids)
                conn.commit()
    finally:
        conn.close()

def watch_and_sync(excel_path: str, db_path: str, purge_missing: bool, interval: float = 2.0):
    last_mtime = None
    print(f"Watching {excel_path} for changes. Press Ctrl+C to stop.")
    while True:
        try:
            mtime = os.path.getmtime(excel_path)
            if last_mtime is None or mtime != last_mtime:
                print("Change detected. Syncing...")
                sync_excel_to_sqlite(excel_path, db_path, purge_missing=purge_missing)
                print("Sync complete.")
                last_mtime = mtime
        except KeyboardInterrupt:
            print("Stopped.")
            return
        except FileNotFoundError:
            print("File not found. Waiting...")
        except Exception as e:
            print(f"Error: {e}")
        time.sleep(interval)

def main():
    parser = argparse.ArgumentParser(description="Sync Excel/CSV skiing products into a normalized SQLite database.")
    parser.add_argument("--excel", required=True, help="Path to input Excel (.xlsx) or CSV file")
    parser.add_argument("--db", default=os.path.join(os.path.dirname(__file__), "data", "ski_products.db"),
                        help="Path to SQLite DB (will be created if missing)")
    parser.add_argument("--purge-missing", action="store_true",
                        help="Mark products not present in the Excel as inactive")
    parser.add_argument("--watch", action="store_true", help="Watch the Excel file and auto-sync on changes")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.db), exist_ok=True)

    if args.watch:
        watch_and_sync(args.excel, args.db, purge_missing=args.purge_missing)
    else:
        sync_excel_to_sqlite(args.excel, args.db, purge_missing=args.purge_missing)
        print(f"Synced {args.excel} -> {args.db}")

if __name__ == "__main__":
    main()