import polars as pl

print("read parquet")
# Read the Parquet file
df = pl.scan_parquet('combined_sentiment.parquet')

print("group by and aggregate")
#Group by 'country' and 'sentiment' and aggregate the counts
grouped = df.group_by(['tweet_language', 'sentiment']).agg([
    pl.sum('quote_count').alias('quote_count'),
    pl.sum('reply_count').alias('reply_count'),
    pl.sum('like_count').alias('like_count'),
    pl.sum('retweet_count').alias('retweet_count'),
    pl.count('sentiment').alias('total_count')
])

#grouped = df.group_by(['userid']).agg([pl.max('follower_count')]).group_by('follower_count').agg([pl.count('userid').alias('count')])

print("write result")
# Write the results to a new CSV file
grouped.sink_csv('sentiment_origin_country_summary.csv')
