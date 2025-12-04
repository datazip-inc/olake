#!/usr/bin/env python3
"""
Generate test files (Parquet, CSV, JSON) for S3 driver testing
Creates sample data with proper schema for testing incremental syncs
Supports multiple formats and compression (gzip)
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import os
import random
import json
import gzip

def generate_users_data(date_str, num_records=100):
    """Generate sample users data"""
    base_date = datetime.strptime(date_str, "%Y-%m-%d")
    
    data = {
        'user_id': range(1, num_records + 1),
        'username': [f'user_{i}' for i in range(1, num_records + 1)],
        'email': [f'user{i}@example.com' for i in range(1, num_records + 1)],
        'created_at': [base_date + timedelta(hours=i) for i in range(num_records)],
        'updated_at': [base_date + timedelta(hours=i, minutes=30) for i in range(num_records)],
        'status': [random.choice(['active', 'inactive', 'pending']) for _ in range(num_records)],
        'age': [random.randint(18, 80) for _ in range(num_records)],
    }
    
    return pd.DataFrame(data)

def generate_orders_data(date_str, num_records=150):
    """Generate sample orders data"""
    base_date = datetime.strptime(date_str, "%Y-%m-%d")
    
    data = {
        'order_id': range(1, num_records + 1),
        'user_id': [random.randint(1, 100) for _ in range(num_records)],
        'product_id': [random.randint(1, 50) for _ in range(num_records)],
        'amount': [round(random.uniform(10.0, 1000.0), 2) for _ in range(num_records)],
        'quantity': [random.randint(1, 10) for _ in range(num_records)],
        'order_date': [base_date + timedelta(hours=i) for i in range(num_records)],
        'updated_at': [base_date + timedelta(hours=i, minutes=15) for i in range(num_records)],
        'status': [random.choice(['pending', 'completed', 'cancelled', 'shipped']) for _ in range(num_records)],
    }
    
    return pd.DataFrame(data)

def generate_products_data(date_str, num_records=50):
    """Generate sample products data"""
    base_date = datetime.strptime(date_str, "%Y-%m-%d")
    
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Toys', 'Sports']
    
    data = {
        'product_id': range(1, num_records + 1),
        'product_name': [f'Product {i}' for i in range(1, num_records + 1)],
        'category': [random.choice(categories) for _ in range(num_records)],
        'price': [round(random.uniform(5.0, 500.0), 2) for _ in range(num_records)],
        'stock': [random.randint(0, 500) for _ in range(num_records)],
        'created_at': [base_date for _ in range(num_records)],
        'updated_at': [base_date + timedelta(hours=i) for i in range(num_records)],
        'is_active': [random.choice([True, False]) for _ in range(num_records)],
    }
    
    return pd.DataFrame(data)

def save_parquet(df, filepath):
    """Save DataFrame as Parquet file"""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    # Convert datetime columns to proper timestamp format
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col])
    
    # Write Parquet file
    table = pa.Table.from_pandas(df)
    pq.write_table(table, filepath, compression='snappy')
    print(f"✓ Created: {filepath} ({len(df)} records)")

def save_csv(df, filepath, compressed=False):
    """Save DataFrame as CSV file (optionally gzipped)"""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    # Convert datetime columns to ISO format strings
    df_copy = df.copy()
    for col in df_copy.columns:
        if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
            df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    if compressed:
        with gzip.open(filepath, 'wt', encoding='utf-8') as f:
            df_copy.to_csv(f, index=False)
        print(f"✓ Created: {filepath} ({len(df)} records, gzipped)")
    else:
        df_copy.to_csv(filepath, index=False)
        print(f"✓ Created: {filepath} ({len(df)} records)")

def save_json(df, filepath, compressed=False, line_delimited=True):
    """Save DataFrame as JSON file (optionally gzipped, line-delimited or array)"""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    # Convert datetime columns to ISO format strings
    df_copy = df.copy()
    for col in df_copy.columns:
        if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
            df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    if line_delimited:
        # JSONL format (one JSON object per line)
        if compressed:
            with gzip.open(filepath, 'wt', encoding='utf-8') as f:
                for _, row in df_copy.iterrows():
                    f.write(json.dumps(row.to_dict()) + '\n')
            print(f"✓ Created: {filepath} ({len(df)} records, JSONL, gzipped)")
        else:
            with open(filepath, 'w', encoding='utf-8') as f:
                for _, row in df_copy.iterrows():
                    f.write(json.dumps(row.to_dict()) + '\n')
            print(f"✓ Created: {filepath} ({len(df)} records, JSONL)")
    else:
        # JSON array format
        records = df_copy.to_dict(orient='records')
        if compressed:
            with gzip.open(filepath, 'wt', encoding='utf-8') as f:
                json.dump(records, f, indent=2)
            print(f"✓ Created: {filepath} ({len(df)} records, JSON array, gzipped)")
        else:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(records, f, indent=2)
            print(f"✓ Created: {filepath} ({len(df)} records, JSON array)")

def main():
    """Generate all test data files in multiple formats"""
    base_dir = "test_data"
    
    print("=== Generating Test Files (Parquet, CSV, JSON) ===\n")
    
    # ==========================================
    # 1. PARQUET FILES (Snappy compression)
    # ==========================================
    print("📦 Generating Parquet files...")
    save_parquet(
        generate_users_data("2024-01-01", 100),
        f"{base_dir}/parquet/users/2024-01-01/user_data.parquet"
    )
    save_parquet(
        generate_users_data("2024-01-02", 120),
        f"{base_dir}/parquet/users/2024-01-02/user_data.parquet"
    )
    save_parquet(
        generate_orders_data("2024-01-01", 150),
        f"{base_dir}/parquet/orders/2024-01-01/order_data.parquet"
    )
    save_parquet(
        generate_orders_data("2024-01-02", 180),
        f"{base_dir}/parquet/orders/2024-01-02/order_data.parquet"
    )
    save_parquet(
        generate_products_data("2024-01-01", 50),
        f"{base_dir}/parquet/products/2024-01-01/product_data.parquet"
    )
    
    # ==========================================
    # 2. CSV FILES (Plain and Gzipped)
    # ==========================================
    print("\n📄 Generating CSV files...")
    # Plain CSV
    save_csv(
        generate_users_data("2024-01-01", 80),
        f"{base_dir}/csv/users/2024-01-01/user_data.csv",
        compressed=False
    )
    # Gzipped CSV
    save_csv(
        generate_users_data("2024-01-02", 90),
        f"{base_dir}/csv/users/2024-01-02/user_data.csv.gz",
        compressed=True
    )
    save_csv(
        generate_orders_data("2024-01-01", 120),
        f"{base_dir}/csv/orders/2024-01-01/order_data.csv",
        compressed=False
    )
    save_csv(
        generate_orders_data("2024-01-02", 140),
        f"{base_dir}/csv/orders/2024-01-02/order_data.csv.gz",
        compressed=True
    )
    save_csv(
        generate_products_data("2024-01-01", 40),
        f"{base_dir}/csv/products/2024-01-01/product_data.csv.gz",
        compressed=True
    )
    
    # ==========================================
    # 3. JSON FILES (Line-delimited and Array, Plain and Gzipped)
    # ==========================================
    print("\n🔤 Generating JSON files...")
    # JSONL (Line-delimited) - Plain
    save_json(
        generate_users_data("2024-01-01", 60),
        f"{base_dir}/json/users/2024-01-01/user_data.jsonl",
        compressed=False,
        line_delimited=True
    )
    # JSONL - Gzipped
    save_json(
        generate_users_data("2024-01-02", 75),
        f"{base_dir}/json/users/2024-01-02/user_data.jsonl.gz",
        compressed=True,
        line_delimited=True
    )
    # JSON Array - Plain
    save_json(
        generate_orders_data("2024-01-01", 100),
        f"{base_dir}/json/orders/2024-01-01/order_data.json",
        compressed=False,
        line_delimited=False
    )
    # JSON Array - Gzipped
    save_json(
        generate_orders_data("2024-01-02", 110),
        f"{base_dir}/json/orders/2024-01-02/order_data.json.gz",
        compressed=True,
        line_delimited=False
    )
    # JSONL for products
    save_json(
        generate_products_data("2024-01-01", 30),
        f"{base_dir}/json/products/2024-01-01/product_data.jsonl.gz",
        compressed=True,
        line_delimited=True
    )
    
    # ==========================================
    # 4. MIXED FORMAT (for testing auto-detection)
    # ==========================================
    print("\n🔀 Generating mixed format data...")
    save_parquet(
        generate_users_data("2024-01-03", 50),
        f"{base_dir}/mixed/transactions/2024-01-03/data.parquet"
    )
    save_csv(
        generate_orders_data("2024-01-03", 60),
        f"{base_dir}/mixed/transactions/2024-01-03/data.csv.gz",
        compressed=True
    )
    save_json(
        generate_products_data("2024-01-03", 20),
        f"{base_dir}/mixed/transactions/2024-01-03/data.jsonl",
        compressed=False,
        line_delimited=True
    )
    
    print(f"\n{'='*60}")
    print(f"=== Test Data Generated Successfully ===")
    print(f"{'='*60}\n")
    
    print("📁 Folder Structure:")
    print(f"""
{base_dir}/
├── parquet/          # Parquet files (Snappy compression)
│   ├── users/        → 2 files, 220 records
│   ├── orders/       → 2 files, 330 records
│   └── products/     → 1 file, 50 records
│
├── csv/              # CSV files (plain + gzip)
│   ├── users/        → 2 files (1 plain, 1 gzipped), 170 records
│   ├── orders/       → 2 files (1 plain, 1 gzipped), 260 records
│   └── products/     → 1 file (gzipped), 40 records
│
├── json/             # JSON files (JSONL + array, plain + gzip)
│   ├── users/        → 2 files (JSONL), 135 records
│   ├── orders/       → 2 files (JSON array), 210 records
│   └── products/     → 1 file (JSONL gzipped), 30 records
│
└── mixed/            # Mixed formats for testing
    └── transactions/ → 3 files (parquet, csv.gz, jsonl), 130 records
    """)
    
    print("\n🎯 Expected Streams (with stream_grouping_level: 1):")
    print("  Parquet: users, orders, products (3 streams)")
    print("  CSV:     users, orders, products (3 streams)")
    print("  JSON:    users, orders, products (3 streams)")
    print("  Mixed:   transactions (1 stream)")
    print("\n✅ Total: 10 distinct streams across all formats")
    
    print("\n💡 Testing Tips:")
    print("  • Test Parquet: path_prefix='parquet/', file_format='parquet'")
    print("  • Test CSV:     path_prefix='csv/', file_format='csv'")
    print("  • Test JSON:    path_prefix='json/', file_format='json'")
    print("  • Test Mixed:   path_prefix='mixed/' (auto-detect)")
    print("  • Gzip compression is automatically detected by file extension")

if __name__ == "__main__":
    main()

