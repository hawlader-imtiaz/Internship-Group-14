
#Count the "review_count" for users.
%pyspark
from pyspark import HiveContext
from pyspark.sql.functions import sum

# Create a HiveContext
hc = HiveContext(sc)

# Read the table 'users' from Hive
users_df = hc.table('users')

# Calculate the total review count
total_review_count = users_df.select(sum("user_review_count")).collect()[0][0]

# Show the total review count
print("Total Review Count:", total_review_count)



#Analyze the yearly growth of user sign-ups.
%pyspark
#       Analyze the yearly growth of user sign-ups.
from pyspark import HiveContext
from pyspark.sql.functions import year, count

# Create a HiveContext
hc = HiveContext(sc)

# Read the table 'users' from Hive
df = hc.table('users')

# Calculate yearly sign-ups for all users
yearly_signups = df.withColumn("signup_year", year("user_yelping_since")) \
    .groupBy("signup_year") \
    .agg(count("*").alias("total_signups")) \
    .orderBy("signup_year")

# Show the yearly sign-ups
z.show(yearly_signups)


#Summarize the yearly statistics for new users
%pyspark

from pyspark import HiveContext
from pyspark.sql.functions import year, count

# Create a HiveContext
hc = HiveContext(sc)

# Read the table 'users' from Hive
df = hc.table('users')

# Calculate yearly statistics for new users
yearly_new_users = df.withColumn("signup_year", year("user_yelping_since")) \
    .groupBy("signup_year") \
    .agg(count("*").alias("new_users")) \
    .orderBy("signup_year")

# Show the yearly statistics for new users
z.show(yearly_new_users)



#Identify and list the most popular users based on their number of fans.

%pyspark
from pyspark import HiveContext
from pyspark.sql.functions import desc

hc = HiveContext(sc)
df = hc.table('users')

# Select "user_id" and "user_fans", and order by "user_fans" in descending order
popular_users = df.select("user_id", "user_fans").orderBy(desc("user_fans"))

# Limit the DataFrame to 10 rows
popular_users_limit = popular_users.limit(10)

# Show the DataFrame with the most popular users first
z.show(popular_users_limit)



#Toatl users and the new user accoring to the year
%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, count, sum

# Create a SparkSession
spark = SparkSession.builder \
    .appName("YelpTotalUserAnalysis") \
    .getOrCreate()

# Read the table 'users' from Hive
df = spark.table('users')

# Extract the signup year from the 'user_yelping_since' column
df_with_year = df.withColumn("signup_year", year("user_yelping_since"))

# Calculate the number of new users for each year
yearly_new_users = df_with_year.groupBy("signup_year") \
    .agg(count("*").alias("new_users")) \
    .orderBy("signup_year")

# Calculate the cumulative total count of users per year
yearly_total_users = yearly_new_users.withColumn("total_users", sum("new_users").over(Window.orderBy("signup_year").rowsBetween(Window.unboundedPreceding, 0)))

# Show the yearly count of total users
z.show(yearly_total_users)

#Yearly Statistics for review counts
%pyspark
from pyspark import HiveContext
from pyspark.sql.functions import year, sum

# Create a HiveContext
hc = HiveContext(sc)

# Read the table 'users' from Hive
df = hc.table('users')

# Calculate yearly statistics for review counts
yearly_review_counts = df.withColumn("signup_year", year("user_yelping_since")) \
    .groupBy("signup_year") \
    .agg(sum("user_review_count").alias("total_review_count")) \
    .orderBy("signup_year")

# Show the yearly statistics for review counts
z.show(yearly_review_counts)


#Display the yearly proportions of total users and silent users (those who haven't written reviews).
%pyspark
from pyspark.sql import HiveContext
from pyspark.sql.functions import year, count
from pyspark.sql.window import Window

# Create a HiveContext
hc = HiveContext(sc)

# Read the table 'users' from Hive
df = hc.table('users')

# Extract the signup year from the 'user_yelping_since' column
df_with_year = df.withColumn("signup_year", year("user_yelping_since"))

# Calculate the number of new users for each year
yearly_new_users = df_with_year.groupBy("signup_year") \
    .agg(count("*").alias("new_users")) \
    .orderBy("signup_year")

# Calculate the cumulative total count of users per year
windowSpec = Window.orderBy("signup_year").rowsBetween(Window.unboundedPreceding, 0)
yearly_total_users = yearly_new_users.withColumn("total_users", sum("new_users").over(windowSpec))

# Calculate the number of silent users for each year
silent_users = df_with_year.where(df_with_year.user_review_count == 0) \
    .groupBy("signup_year") \
    .agg(count("*").alias("silent_users")) \
    .orderBy("signup_year")

# Join the yearly_total_users with silent_users to calculate silent user proportion
yearly_proportions = yearly_total_users.join(silent_users, "signup_year", "left_outer") \
    .withColumn("silent_user_proportion", (silent_users["silent_users"] / yearly_total_users["total_users"])) \
    .orderBy("signup_year") \
    .select("signup_year", "total_users", "silent_user_proportion")

# Show the yearly proportions
z.show(yearly_proportions)



#Calculate the ratio of elite users to regular users each year.
%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, count, when, sum, col
from pyspark.sql.window import Window


# Read the table 'users' from Hive
df = spark.table('users')

# Extract the year from the 'yelping_since' column
df_with_year = df.withColumn("signup_year", year("user_yelping_since"))

# Calculate the number of new users for each year
yearly_new_users = df_with_year.groupBy("signup_year") \
    .agg(count("*").alias("new_users")) \
    .orderBy("signup_year")

# Calculate the cumulative total count of users per year
windowSpec = Window.orderBy("signup_year").rowsBetween(Window.unboundedPreceding, 0)
yearly_total_users = yearly_new_users.withColumn("total_users", sum("new_users").over(windowSpec))

# Calculate the number of elite users for each year
elite_user_counts = df_with_year.filter(df_with_year["user_elite"] != "") \
    .groupBy("signup_year") \
    .agg(count("*").alias("elite_users")) \
    .orderBy("signup_year")

# Join elite user counts and total user counts
user_counts = yearly_total_users.join(elite_user_counts, "signup_year", "left_outer") \
    .withColumn("regular_users", when(col("elite_users").isNull(), col("total_users"))
                .otherwise(col("total_users") - col("elite_users"))) \
    .select("signup_year", "regular_users", "elite_users") \
    .orderBy("signup_year")

# Calculate the ratio of elite users to regular users each year
user_counts_with_ratio = user_counts.withColumn("elite_to_regular_ratio", col("elite_users") / col("regular_users"))

# Show the top 20 rows of the DataFrame
z.show(user_counts_with_ratio, numRows=20)

