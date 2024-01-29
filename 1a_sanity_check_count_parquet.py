import pyarrow.parquet as pq

parquet_file_path = 'combined.parquet'
parquet_file = pq.ParquetFile(parquet_file_path)

print(f"Number of entries (rows) in the Parquet file: {parquet_file.metadata.num_rows:,d}")
