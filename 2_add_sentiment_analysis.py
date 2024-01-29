import polars as pl
from textblob import TextBlob

# Function to perform sentiment analysis
def analyze_sentiment(tweet):
    if tweet is None or tweet == "":
        return 'neutral'
    
    analysis = TextBlob(tweet)
    if analysis.sentiment.polarity > 0:
        return 'positive'
    elif analysis.sentiment.polarity == 0:
        return 'neutral'
    else:
        return 'negative'

print("read parquet")
# Read the Parquet file
df = pl.scan_parquet('combined.parquet')

print("apply sentiment")
df = df.with_columns(
    pl.col("tweet_text").map_elements(analyze_sentiment).alias('sentiment')
)

print("write parquet")
# Collect the lazy frame into an eager frame and write to a Parquet file
df.collect().write_parquet('combined_sentiment.parquet')