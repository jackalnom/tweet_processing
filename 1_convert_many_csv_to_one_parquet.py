import pyarrow.csv as csv
import pyarrow.parquet as pq
import glob
import os
import pyarrow as pa
from pyarrow import parquet

root_folder_path = 'tweets/'  
parquet_file = 'combined.parquet'

csv_files = glob.glob(os.path.join(root_folder_path, '**/*.csv'), recursive=True)

columns_to_read = [
    'tweetid', 'userid', 'follower_count', 'following_count', 'account_creation_date', 'account_language',
    'tweet_text', 'tweet_language', 'tweet_time', 'quote_count', 'reply_count', 'like_count', 'retweet_count']

# final schema to write to parquet
schema = pa.schema([
    pa.field('tweetid', pa.int64()),
    pa.field('userid', pa.string()),
    pa.field('follower_count', pa.int64()),
    pa.field('following_count', pa.int64()),
    pa.field('account_creation_date', pa.date32()),
    pa.field('account_language', pa.string()),
    pa.field('tweet_text', pa.string()),
    pa.field('tweet_language', pa.string()),
    pa.field('tweet_time', pa.timestamp('s')),
    pa.field('quote_count', pa.int64()),
    pa.field('reply_count', pa.int64()),
    pa.field('like_count', pa.int64()),
    pa.field('retweet_count', pa.int64())
])

writer = parquet.ParquetWriter(parquet_file, schema, compression='snappy')

# support multi-line CSVs
parse_options = csv.ParseOptions(newlines_in_values=True)
# only read particular columns that we will want. This also helps on 
# what data canonicalization we need to perform.
convert_options=csv.ConvertOptions(include_columns=columns_to_read)

for file in csv_files:
    print("reading", file)
    table = csv.read_csv(file, parse_options=parse_options, convert_options=convert_options)
    table = table.cast(schema)
    print("writing", file)

    # Append table to Parquet file
    writer.write_table(table)

# Close the Parquet writer
if writer:
    writer.close()

print(f"Combined CSVs to {parquet_file}")