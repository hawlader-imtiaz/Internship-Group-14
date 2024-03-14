#Identify the cities where check-ins are most frequent.

%pyspark
from pyspark import HiveContext
from pyspark.sql.functions import count, col

# Create a HiveContext
hc = HiveContext(sc)

# Load the 'checkin' dataset
checkin_df = hc.table("checkin")

# Load the 'business' dataset
business_df = hc.table("business")

# Join the two datasets on the 'business_id' column
joined_df = checkin_df.join(business_df, checkin_df.business_id == business_df.business_id)

# Group by city and count the number of check-ins
city_checkin_frequency = joined_df.groupBy("city").agg(count("*").alias("checkin_count")).orderBy(col("checkin_count").desc())

# Show the top 20 cities with the most frequent check-ins
z.show(city_checkin_frequency.limit(20))



#Create a ranking of all merchants based on check-in frequency.
%pyspark
from pyspark import HiveContext
from pyspark.sql.functions import count, col

# Create a HiveContext
hc = HiveContext(sc)

# Load the 'checkin' dataset
checkin_df = hc.table("checkin")

# Load the 'business' dataset
business_df = hc.table("business")

# Join the two datasets on the 'business_id' column
joined_df = checkin_df.join(business_df, checkin_df.business_id == business_df.business_id)

# Group by merchant name and count the number of check-ins
merchant_checkin_frequency = joined_df.groupBy("name").agg(count("*").alias("checkin_count")).orderBy(col("checkin_count").desc())

# Set limit to 20 for displaying top merchants
top_merchants = merchant_checkin_frequency.limit(20)

# Show the top 20 merchants with the most frequent check-ins
z.show(top_merchants)



#Count the yearly check-in frequency.
%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, to_date

# Create a SparkSession
spark = SparkSession.builder \
    .appName("YearlyCheckinFrequency") \
    .enableHiveSupport() \
    .getOrCreate()

# Load the 'checkin' table from Hive
checkin_df = spark.table("checkin")

# Convert the string representation of dates to date type
checkin_df = checkin_df.withColumn("checkin_date", to_date("checkin_dates", "yyyy-MM-dd HH:mm:ss"))

# Calculate the yearly check-in frequency
yearly_checkin_freq = checkin_df.withColumn("Year", year("checkin_date")) \
    .groupBy("Year").count().orderBy("Year")

# Show the yearly check-in frequency using z.show()
z.show(yearly_checkin_freq)


#Analyze the check-in frequency for each hour of the day.
%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, count

# Create a SparkSession
spark = SparkSession.builder \
    .appName("HourlyCheckinFrequency") \
    .enableHiveSupport() \
    .getOrCreate()

# Load the 'checkin' table from Hive
checkin_df = spark.table("checkin")

# Convert the 'checkin_dates' column to timestamp type
checkin_df = checkin_df.withColumn("checkin_timestamp", to_timestamp("checkin_dates", "yyyy-MM-dd HH:mm:ss"))

# Extract the hour of the day from the timestamp
checkin_df = checkin_df.withColumn("HourOfDay", hour("checkin_timestamp"))

# Calculate the check-in frequency for each hour of the day
hourly_checkin_freq = checkin_df.groupBy("HourOfDay").agg(count("*").alias("CheckinFrequency")).orderBy("HourOfDay")

# Show the hourly check-in frequency using z.show()
z.show(hourly_checkin_freq)
