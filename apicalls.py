from fastapi import FastAPI, HTTPException, Query
import sqlite3
from typing import List
from pydantic import BaseModel
import os

# Pydantic model for response data
class Product(BaseModel):
    id: int
    name: str
    price: float
    stock: int

# Initialize FastAPI app
app = FastAPI(
    title="Ski Products API",
    description="Simple API for ski products - returns only id, name, price and stock",
    version="1.0.0"
)

# Database connection helper
def get_db_connection():
    db_path = os.path.join(os.path.dirname(__file__), 'data', 'ski_products.db')
    if not os.path.exists(db_path):
        raise HTTPException(status_code=500, detail="Database not found. Please run create_database.py first.")
    return sqlite3.connect(db_path)

# Root endpoint
@app.get("/")
async def root():
    return {"message": "Ski Products API - Returns id, name, price, stock only"}

# Get all products
@app.get("/products", response_model=List[Product])
async def get_all_products():
    """Get all products with id, name, price and stock"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                p.id,
                p.name,
                p.price,
                i.stock_quantity
            FROM products p
            JOIN inventory i ON p.id = i.product_id
            WHERE p.is_active = 1
            ORDER BY p.id
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()
        
        products = [
            Product(id=row[0], name=row[1], price=row[2], stock=row[3])
            for row in rows
        ]
        
        return products
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# Get product by ID
@app.get("/products/{product_id}", response_model=Product)
async def get_product_by_id(product_id: int):
    """Get product by ID - returns id, name, price and stock"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                p.id,
                p.name,
                p.price,
                i.stock_quantity
            FROM products p
            JOIN inventory i ON p.id = i.product_id
            WHERE p.id = ? AND p.is_active = 1
        """
        
        cursor.execute(query, (product_id,))
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            raise HTTPException(status_code=404, detail=f"Product with ID {product_id} not found")
        
        product = Product(id=row[0], name=row[1], price=row[2], stock=row[3])
        return product
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# Get just product name by ID
@app.get("/products/{product_id}/name")
async def get_product_name(product_id: int):
    """Get just the product name by ID"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM products WHERE id = ? AND is_active = 1", (product_id,))
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            raise HTTPException(status_code=404, detail=f"Product with ID {product_id} not found")
        
        return {"name": row[0]}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# Get just product price by ID
@app.get("/products/{product_id}/price")
async def get_product_price(product_id: int):
    """Get just the product price by ID"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT price FROM products WHERE id = ? AND is_active = 1", (product_id,))
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            raise HTTPException(status_code=404, detail=f"Product with ID {product_id} not found")
        
        return {"price": row[0]}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# Get just product stock by ID
@app.get("/products/{product_id}/stock")
async def get_product_stock(product_id: int):
    """Get just the product stock by ID"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT i.stock_quantity
            FROM inventory i
            JOIN products p ON i.product_id = p.id
            WHERE p.id = ? AND p.is_active = 1
        """
        
        cursor.execute(query, (product_id,))
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            raise HTTPException(status_code=404, detail=f"Product with ID {product_id} not found")
        
        return {"stock": row[0]}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, port=8000)