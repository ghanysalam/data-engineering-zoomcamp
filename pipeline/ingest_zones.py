import pandas as pd
from sqlalchemy import create_engine
import os

def main():
    # 1. Konfigurasi Database (Sesuaikan dengan docker-compose kamu)
    # Karena dijalankan dari terminal Codespace (Host), gunakan 'localhost'
    user = 'root'
    password = 'root'
    host = 'localhost'
    port = '5432'
    db = 'ny_taxi'
    table_name = 'zones'
    
    # URL Data Zona
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

    try:
        # 2. Buat koneksi ke Postgres
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        
        print(f"[*] Mendownload data dari: {url}")
        df_zones = pd.read_csv(url)
        
        print(f"[*] Data terbaca: {len(df_zones)} baris.")
        
        # 3. Masukkan ke Database
        # 'replace' artinya jika tabel sudah ada, akan dihapus dan dibuat ulang
        print(f"[*] Memasukkan data ke tabel '{table_name}'...")
        df_zones.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        
        print("[V] Berhasil! Tabel 'zones' sudah siap digunakan.")
        
    except Exception as e:
        print(f"[X] Terjadi kesalahan: {e}")

if __name__ == "__main__":
    main()