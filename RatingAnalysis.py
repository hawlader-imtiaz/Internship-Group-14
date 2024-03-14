#Analyze the distribution of ratings (1-5).
%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import HiveContext

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Rating Analysis") \
    .getOrCreate()

# Create a HiveContext
hc = HiveContext(spark.sparkContext)

# Load the review dataset from Hive
df = hc.table('review')

# Group by the 'rev_stars' column and count the occurrences of each rating
rating_distribution = df.groupBy("rev_stars").count().orderBy("rev_stars")

# Show the distribution of ratings
z.show(rating_distribution)



#Count the frequency of ratings on each day of the week.
%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, dayofweek
from pyspark.sql import HiveContext

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Rating Frequency by Day of Week") \
    .getOrCreate()

# Create a HiveContext
hc = HiveContext(spark.sparkContext)

# Load the review dataset from Hive
df = hc.table('review')

# Extract the day of the week from the 'rev_date' column
df = df.withColumn("day_of_week", date_format("rev_date", "EEEE"))

# Group by the day of the week and 'rev_stars' column, and count the occurrences of each rating
rating_frequency_by_day_of_week = df.groupBy("day_of_week", "rev_stars").count().orderBy("day_of_week", "rev_stars")

# Show the frequency of ratings on each day of the week
z.show(rating_frequency_by_day_of_week)



#Identify the top 5 merchants with the most 5-star ratings.

%pyspark
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Top 5 Merchants with Most 5-Star Ratings") \
    .enableHiveSupport() \
    .getOrCreate()

# Load the review and business datasets from Hive
df_review = spark.table('review')
df_business = spark.table('business')

# Filter the reviews with 5-star ratings
df_5_star_ratings = df_review.filter(df_review["rev_stars"] == 5)

# Group by business_id and count the occurrences of 5-star ratings for each merchant
top_merchants_with_most_5_stars = df_5_star_ratings.groupBy("rev_business_id") \
    .count().orderBy("count", ascending=False)

# Join with the business table to get the merchant names
top_merchants_with_names = top_merchants_with_most_5_stars.join(df_business.select("business_id", "name"),
                                                               top_merchants_with_most_5_stars["rev_business_id"] == df_business["business_id"])

# Select the top 5 merchants with the most 5-star ratings
top_5_merchants = top_merchants_with_names.select("name", "count").limit(5)

# Show the top 5 merchants with the most 5-star ratings
z.show(top_5_merchants)

# Stop the SparkSession
spark.stop()

