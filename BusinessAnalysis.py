

#Identify the most common merchants in the United States (Top 20).
import pyspark
%pyspark

from pyspark import HiveContext
from pyspark.sql.functions import count, col
hc = HiveContext(sc)
df = hc.table('business')

result = df.groupBy('name')\
    .agg(count('name').alias('cnt'))\
    .orderBy(col('cnt').desc())\
    .limit(20)

z.show(result)

#categories count
%pyspark
from pyspark.sql import HiveContext
from pyspark.sql.functions import count, col, split, explode
hc = HiveContext(sc)
df = hc.table('business')

result = df.select(explode(split('categories', ', ')).alias('category'))\
    .groupBy('category')\
    .agg(count('category').alias('cnt'))\
    .orderBy(col('cnt').desc())\
    .limit(10)

z.show(result)

#City Based Merchant Count
%pyspark
from pyspark.sql import HiveContext
from pyspark.sql.functions import count, col
sc = spark.sparkContext
hc = HiveContext(sc)
df = hc.table('business')

# Perform aggregation by city
result = df.groupBy('city') \
            .agg(count('name').alias('merchant_count')) \
            .orderBy(col('merchant_count').desc()) \
            .limit(10)

z.show(result)

#Average Rating of The maerchant Count

%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, avg
sc = spark.sparkContext
hc = HiveContext(sc)
df = hc.table('business')
result = df.groupBy('name') \
                      .agg(count('name').alias('merchant_count'), avg('stars').alias('avg_rating')) \
                      .orderBy(col('merchant_count').desc()) \
                      .limit(20)

z.show(result)

#State wise Merchant count
%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col
sc = spark.sparkContext
hc = HiveContext(sc)
df = hc.table('business')
# Perform aggregation by state
result = df.groupBy('state') \
                      .agg(count('name').alias('merchant_count')) \
                      .orderBy(col('merchant_count').desc()) \
                      .limit(5)

z.show(result)

#categories count top 10

%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, count, col
sc = spark.sparkContext
hc = HiveContext(sc)
df = hc.table('business')

result = df.select(explode(split('categories', ', ')).alias('category')) \
           .groupBy('category') \
           .agg(count('*').alias('cnt')) \
           .orderBy(col('cnt').desc()) \
           .limit(10)

z.show(result)

#five-star reviews

%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
sc = spark.sparkContext
hc = HiveContext(sc)
df = hc.table('business')


# Filter businesses with five-star reviews
five_star_businesses = df.filter(df.stars == 5)

result = five_star_businesses.groupBy('name') \
                              .agg(sum('review_count').alias('five_star_reviews')) \
                              .orderBy(col('five_star_reviews').desc()) \
                              .limit(20)

z.show(result)

#Identify the top 10 cities with the highest average review counts per merchant in the United States

%pyspark
from pyspark import HiveContext
from pyspark.sql.functions import count, col
hc = HiveContext(sc)
df = hc.table('business')
result = df.filter(df['is_open'] == 1)\
    .groupBy('city')\
    .agg((count('review_count') / count('name')).alias('avg_review_count_per_merchant'))\
    .orderBy(col('avg_review_count_per_merchant').desc())\
    .limit(30)

z.show(result)

#Determine the average latitude and longitude coordinates for businesses in each state:
%pyspark

from pyspark import HiveContext
from pyspark.sql.functions import count, col , avg
hc = HiveContext(sc)
df = hc.table('business')
result = df.groupBy('state')\
    .agg(avg('latitude').alias('avg_latitude'), avg('longitude').alias('avg_longitude'))

z.show(result)



#Summarize the types and quantities of restaurants for different cuisines (Chinese, American, Mexican).

%pyspark

from pyspark.sql import HiveContext
from pyspark.sql.functions import col, when

# Create HiveContext
hc = HiveContext(sc)

# Load the 'business' table
business_df = hc.table('business')

# Define the cuisine types
cuisines = ['Chinese', 'American', 'Mexican']

# Create columns for each cuisine type indicating whether the business belongs to that cuisine
business_with_cuisines = business_df.withColumn('Chinese', when(col('categories').contains('Chinese'), 1).otherwise(0)) \
                                   .withColumn('American', when(col('categories').contains('American'), 1).otherwise(0)) \
                                   .withColumn('Mexican', when(col('categories').contains('Mexican'), 1).otherwise(0))

# Summarize the types and quantities of restaurants for different cuisines
cuisine_summary = business_with_cuisines.selectExpr('sum(Chinese) as Chinese',
                                                    'sum(American) as American',
                                                    'sum(Mexican) as Mexican')

z.show(cuisine_summary)

#Analyze the review counts for restaurants of different cuisines.
%pyspark

from pyspark import HiveContext
from pyspark.sql.functions import col

hc = HiveContext(sc)
df = hc.table('business')

# Filter restaurants with specified cuisines
cuisine_types = ['Chinese', 'American', 'Mexican']
restaurant_review_counts = df.select('categories', 'review_count') \
    .filter(df['categories'].like('%Chinese%') | df['categories'].like('%American%') | df['categories'].like('%Mexican%')) \
    .orderBy(col('review_count').desc()) \
    .limit(20)

z.show(restaurant_review_counts)

#Explore the distribution of ratings for restaurants of different cuisines.
%pyspark
from pyspark import HiveContext
from pyspark.sql.functions import col

hc = HiveContext(sc)
df = hc.table('business')

# Filter restaurants with specified cuisines
cuisine_types = ['Chinese', 'American', 'Mexican']
restaurant_ratings = df.select('categories', 'stars') \
    .filter(df['categories'].like('%Chinese%') | df['categories'].like('%American%') | df['categories'].like('%Mexican%')) \
    .orderBy(col('stars').desc()) \
    .limit(10)

z.show(restaurant_ratings)
