import pandas as pd
from sqlalchemy import create_engine, text
import pymongo
import os

# ================= CONFIGURATION =================
# 1. Path to where you unzipped the Olist files
DATA_FOLDER = r"C:\Users\tgundepalli\Downloads\ecom" 

# 2. Connection Strings (Paste yours inside the quotes)
# NEON: postgres://user:pass@ep-xyz.neon.tech/neondb?sslmode=require
NEON_CONN_STR = "postgresql://neondb_owner:npg_jDlYJnBF27UX@ep-young-fire-a82qxqz4-pooler.eastus2.azure.neon.tech/neondb?sslmode=require&channel_binding=require"

# MONGO: mongodb+srv://admin_loader:PASSWORD@cluster0...
MONGO_CONN_STR = "mongodb+srv://admin:Sinchan321@cluster0.9vpczui.mongodb.net/?appName=Cluster0"
# =================================================

def seed_postgres():
    print("\n🚀 Starting SQL Load to Neon...")
    
    # Create Engine (We replace postgres:// with postgresql:// for SQLAlchemy support)
    engine = create_engine(NEON_CONN_STR.replace("postgres://", "postgresql://"))
    
    # Map CSV filenames to Table Names
    files_map = {
        "olist_customers_dataset.csv": "olist_customers",
        "olist_sellers_dataset.csv": "olist_sellers",
        "olist_products_dataset.csv": "olist_products",
        "olist_orders_dataset.csv": "olist_orders",
        "olist_order_items_dataset.csv": "olist_order_items",
        "olist_order_payments_dataset.csv": "olist_order_payments"
    }

    with engine.connect() as conn:
        for csv_file, table_name in files_map.items():
            path = os.path.join(DATA_FOLDER, csv_file)
            if os.path.exists(path):
                print(f"   Reading {csv_file}...")
                df = pd.read_csv(path)
                
                # Fix Date Columns (Postgres will crash if we send '2017-01-01' as text)
                for col in df.columns:
                    if "timestamp" in col or "date" in col:
                        df[col] = pd.to_datetime(df[col], errors='coerce')

                print(f"   --> Uploading {len(df)} rows to '{table_name}'...")
                # chunksize=1000 prevents memory errors
                df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=1000)
                print(f"   ✅ {table_name} Loaded.")
            else:
                print(f"   ❌ FILE NOT FOUND: {csv_file}")

def seed_mongo():
    print("\n🚀 Starting NoSQL Load to MongoDB...")
    
    try:
        client = pymongo.MongoClient(MONGO_CONN_STR)
        db = client["OlistEcomm"]
        collection = db["Reviews"]
        
        path = os.path.join(DATA_FOLDER, "olist_order_reviews_dataset.csv")
        if os.path.exists(path):
            print("   Reading reviews file...")
            df = pd.read_csv(path)
            
            # Convert to JSON format (Records)
            data = df.to_dict(orient='records')
            
            print(f"   --> Inserting {len(data)} documents...")
            collection.insert_many(data)
            print("   ✅ MongoDB Load Complete!")
        else:
            print("   ❌ Reviews file not found.")
            
    except Exception as e:
        print(f"   ❌ Mongo Error: {e}")

if __name__ == "__main__":
    # Install requirements first: 
    # pip install pandas sqlalchemy psycopg2-binary pymongo
    seed_postgres()
    seed_mongo()
