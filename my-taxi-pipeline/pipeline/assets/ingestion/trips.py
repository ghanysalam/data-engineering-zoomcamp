"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
@bruin"""

import os
import json
import pandas as pd

def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    
    bruin_vars = os.environ.get("BRUIN_VARS", "{}")
    taxi_types = json.loads(bruin_vars).get("taxi_types", ["yellow"])

    # Konversi tanggal dan generate daftar bulan (YYYY-MM)
    start = pd.to_datetime(start_date).replace(day=1)
    end = pd.to_datetime(end_date)
    months_to_fetch = pd.date_range(start=start, end=end, freq='MS').strftime("%Y-%m").tolist()

    dfs = []

    for taxi_type in taxi_types:
        for year_month in months_to_fetch:
            # Menggunakan direct link resmi dari NYC TLC website (Format Parquet)
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year_month}.parquet"
            
            print(f"Membaca data dari: {url}...")
            
            try:
                # Membaca langsung file parquet dari URL
                df = pd.read_parquet(url)
                
                # Standarisasi nama kolom agar sesuai dengan layer staging DuckDB
                if 'tpep_pickup_datetime' in df.columns:
                    df = df.rename(columns={
                        'tpep_pickup_datetime': 'pickup_datetime', 
                        'tpep_dropoff_datetime': 'dropoff_datetime',
                        'PULocationID': 'pickup_location_id',
                        'DOLocationID': 'dropoff_location_id'
                    })
                elif 'lpep_pickup_datetime' in df.columns:
                    df = df.rename(columns={
                        'lpep_pickup_datetime': 'pickup_datetime', 
                        'lpep_dropoff_datetime': 'dropoff_datetime',
                        'PULocationID': 'pickup_location_id',
                        'DOLocationID': 'dropoff_location_id'
                    })
                
                # Pastikan format kolom datetime (Parquet biasanya sudah rapi, tapi untuk jaga-jaga)
                df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
                df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])
                
                # Tambahkan label tipe taksi
                df['taxi_type'] = taxi_type 
                dfs.append(df)
                
            except Exception as e:
                print(f"Gagal mengambil data {url}: {e}")

    if dfs:
        final_dataframe = pd.concat(dfs, ignore_index=True)
    else:
        print("Peringatan: Tidak ada data yang berhasil diambil.")
        final_dataframe = pd.DataFrame(columns=[
            'pickup_datetime', 'dropoff_datetime', 'pickup_location_id', 
            'dropoff_location_id', 'fare_amount', 'payment_type', 'taxi_type'
        ])

    return final_dataframe