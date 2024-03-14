#Count the yearly number of reviews.
%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, count
hc = HiveContext(sc)
df = hc.table('review')

# Calculate the yearly review count based on the 'rev_date' column
yearly_review_count = df.withColumn("year", year(df["rev_date"])) \
    .groupBy("year") \
    .agg(count("review_id").alias("review_count")) \
    .orderBy("year", ascending=False)  # Sort by year in descending order

# Show the results
z.show(yearly_review_count)

#Summarize the count of helpful, funny, and cool reviews each year.
%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, sum
hc = HiveContext(sc)
df = hc.table('review')
# Extract the year from the 'rev_date' column
df = df.withColumn("year", year(df["rev_date"]))

# Group by year and summarize the counts of helpful, funny, and cool reviews
summary_by_year = df.groupBy("year") \
    .agg(sum("rev_useful").alias("total_helpful_reviews"),
         sum("rev_funny").alias("total_funny_reviews"),
         sum("rev_cool").alias("total_cool_reviews")) \
    .orderBy("year", ascending=False)  # Sort by year in descending order

# Show the results
z.show(summary_by_year)


#Create a ranking of users based on their total reviews each year.
%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, year, desc, first
from pyspark.sql import HiveContext

# Create a HiveContext
hc = HiveContext(spark.sparkContext)

# Load the review dataset from Hive
df = hc.table('review')

# Extract the yea
r from the 'rev_date' column
df = df.withColumn("year", year(df["rev_date"]))

# Group by user_id and year, and count the number of reviews
user_yearly_reviews = df.groupBy("rev_user_id", "year") \
    .agg(countDistinct("review_id").alias("total_reviews"))

# Define a window specification to rank users within each year based on total reviews
window_spec = Window.partitionBy("year").orderBy(desc("total_reviews"))

# Add a rank column based on total_reviews within each year
user_ranking = user_yearly_reviews.withColumn("rank", dense_rank().over(window_spec))

# Select the top reviewer (first row) for each year
top_reviewers_by_year = user_ranking.filter("rank == 1")

# Show the results
z.show(top_reviewers_by_year.orderBy("year"))


#Extract the top 20 common words from reviews.
%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, regexp_replace
from pyspark.sql.functions import desc

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Top 20 Words") \
    .enableHiveSupport() \
    .getOrCreate()

# Load the review dataset from Hive
df = spark.table('review')

# Tokenize the reviews into individual words
words_df = df.select(explode(split(lower(regexp_replace("rev_text", "[^a-zA-Z\\s]", "")), "\\s+")).alias("word"))

# Count the occurrences of each word in the entire dataset
word_count_df = words_df.groupBy("word").count()

# Sort the words by their frequency in descending order
top_words_df = word_count_df.orderBy(desc("count"))

# Show the top 20 most common words using z.show()
z.show(top_words_df.limit(20))

