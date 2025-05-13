import pandas as pd
import sys

def read_parquet(file_path):
    print(f"\nReading file: {file_path}")
    df = pd.read_parquet(file_path)
    print("\nData types:")
    print(df.dtypes)
    print("\nData:")
    print(df)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python read_parquet.py <parquet_file>")
        sys.exit(1)
    read_parquet(sys.argv[1]) 