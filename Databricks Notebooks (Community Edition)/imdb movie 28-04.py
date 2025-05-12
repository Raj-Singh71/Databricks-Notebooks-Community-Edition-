# Databricks notebook source
# MAGIC %md
# MAGIC # <font color = blue> IMDb Movie EDA </font>
# MAGIC
# MAGIC You have the data for the 100 top-rated movies from the past decade along with various pieces of information about the movie, its actors, and the voters who have rated these movies online. In this assignment, you will try to find some interesting insights into these movies and their voters, using Python.

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IMDB Movies").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Task 1: Reading the data

# COMMAND ----------

# MAGIC %md
# MAGIC - ### Subtask 1.1: Read the Movies Data.
# MAGIC
# MAGIC Read the movies data file provided and store it in a dataframe `movies`.

# COMMAND ----------

file_path = "dbfs:/FileStore/tables/IMDB_Movies.csv"
movies = spark.read.csv(file_path, header=True, inferSchema=True)

# COMMAND ----------

movies.head()

# COMMAND ----------

# MAGIC %md
# MAGIC - ###  Subtask 1.2: Inspect the Dataframe
# MAGIC
# MAGIC Inspect the dataframe for dimensions, null-values, and summary of different numeric columns.

# COMMAND ----------

# Check the number of rows and columns in the dataframe
movies.count(), len(movies.columns)



# COMMAND ----------

# Check the column-wise info of the dataframe
movies.printSchema()




# COMMAND ----------

# Check the summary for the numeric columns 
movies.describe().show()



# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Data Analysis
# MAGIC
# MAGIC Now that we have loaded the dataset and inspected it, we see that most of the data is in place. As of now, no data cleaning is required, so let's start with some data manipulation, analysis, and visualisation to get various insights about the data. 

# COMMAND ----------

# MAGIC %md
# MAGIC -  ###  Subtask 2.1: Reduce those Digits!
# MAGIC
# MAGIC These numbers in the `budget` and `gross` are too big, compromising its readability. Let's convert the unit of the `budget` and `gross` columns from `$` to `million $` first.

# COMMAND ----------

# Divide the 'gross' and 'budget' columns by 1000000 to convert '$' to 'million $'
from pyspark.sql.functions import col, desc, year, to_date

movies = movies.withColumn("gross", col("gross") / 1_000_000)
movies = movies.withColumn("budget", col("budget") / 1_000_000)


# COMMAND ----------

movies.head()

# COMMAND ----------

# MAGIC %md
# MAGIC -  ###  Subtask 2.2: Let's Talk Profit!
# MAGIC
# MAGIC     1. Create a new column called `profit` which contains the difference of the two columns: `gross` and `budget`.
# MAGIC     2. Sort the dataframe using the `profit` column as reference.
# MAGIC     3. Extract the top ten profiting movies in descending order and store them in a new dataframe - `top10`.
# MAGIC     4. Plot a scatter or a joint plot between the columns `budget` and `profit` and write a few words on what you observed.
# MAGIC     5. Extract the movies with a negative profit and store them in a new dataframe - `neg_profit`

# COMMAND ----------

# Create the new column named 'profit' by subtracting the 'budget' column from the 'gross' column

movies = movies.withColumn("profit", col("gross") - col("budget"))



# COMMAND ----------

# Sort the dataframe with the 'profit' column as reference using the 'sort_values' function. Make sure to set the argument

movies = movies.orderBy(col("profit").desc())




# COMMAND ----------

# Get the top 10 profitable movies by using position based indexing. Specify the rows till 10 (0-9)

top_10_profitable = movies.limit(10)
top_10_profitable.select("Title", "title_year", "gross", "budget", "profit").display()




# COMMAND ----------

#Plot profit vs budget using scatterplot

display(movies.select("budget", "profit"))



# COMMAND ----------

# MAGIC %md
# MAGIC The dataset contains the 100 best performing movies from the year 2010 to 2016. However scatter plot tells a different story. You can notice that there are some movies with negative profit. Although good movies do incur losses, but there appear to be quite a few movie with losses. What can be the reason behind this? Lets have a closer look at this by finding the movies with negative profit.

# COMMAND ----------

#Find the movies with negative profit

negative_profit_movies = movies.filter(col("profit") < 0)
negative_profit_movies.select("Title", "profit").show(truncate=False)




# COMMAND ----------

# MAGIC %md
# MAGIC **`Checkpoint 1:`** Can you spot the movie `Tangled` in the dataset? You may be aware of the movie 'Tangled'. Although its one of the highest grossing movies of all time, it has negative profit as per this result. If you cross check the gross values of this movie (link: https://www.imdb.com/title/tt0398286/), you can see that the gross in the dataset accounts only for the domestic gross and not the worldwide gross. This is true for may other movies also in the list.

# COMMAND ----------

# MAGIC %md
# MAGIC - ### Subtask 2.3: The General Audience and the Critics
# MAGIC
# MAGIC You might have noticed the column `MetaCritic` in this dataset. This is a very popular website where an average score is determined through the scores given by the top-rated critics. Second, you also have another column `IMDb_rating` which tells you the IMDb rating of a movie. This rating is determined by taking the average of hundred-thousands of ratings from the general audience. 
# MAGIC
# MAGIC As a part of this subtask, you are required to find out the highest rated movies which have been liked by critics and audiences alike.
# MAGIC 1. Firstly you will notice that the `MetaCritic` score is on a scale of `100` whereas the `IMDb_rating` is on a scale of 10. First convert the `MetaCritic` column to a scale of 10.
# MAGIC 2. Now, to find out the movies which have been liked by both critics and audiences alike and also have a high rating overall, you need to -
# MAGIC     - Create a new column `Avg_rating` which will have the average of the `MetaCritic` and `Rating` columns
# MAGIC     - Retain only the movies in which the absolute difference(using abs() function) between the `IMDb_rating` and `Metacritic` columns is less than 0.5. Refer to this link to know how abs() funtion works - https://www.geeksforgeeks.org/abs-in-python/ .
# MAGIC     - Sort these values in a descending order of `Avg_rating` and retain only the movies with a rating equal to higher than `8` and store these movies in a new dataframe `UniversalAcclaim`.
# MAGIC     

# COMMAND ----------

# Change the scale of MetaCritic column from 100 to 10
movies = movies.withColumn("MetaCritic", (col("MetaCritic") / 10))


# COMMAND ----------

# Find the average ratings including MetaCritic and IMDb_rating
from pyspark.sql import functions as F
movies = movies.withColumn(
    "Avg_rating",
    (F.col("MetaCritic") + F.col("IMDb_rating")) / 2
)
movies.select("Title", "MetaCritic", "IMDb_rating", "Avg_rating").show(5)




# COMMAND ----------

#Sort in descending order of average rating

movies_sorted = movies.orderBy(col("Avg_rating").desc())
movies_sorted.select("Title", "Avg_rating").show(10, truncate=False)



# COMMAND ----------

# Find the movies with metacritic-rating < 0.5 and also with the average rating of > 8

filtered_movies = movies.filter(
    (col("MetaCritic") < 0.5) & (col("Avg_rating") > 8)
)

filtered_movies.select("Title", "MetaCritic", "Avg_rating").show(truncate=False)




# COMMAND ----------

# MAGIC %md
# MAGIC **`Checkpoint 2:`** Can you spot a `Star Wars` movie in your final dataset?

# COMMAND ----------

# MAGIC %md
# MAGIC - ### Subtask 2.4: Find the Most Popular Trios - I
# MAGIC
# MAGIC You're a producer looking to make a blockbuster movie. There will primarily be three lead roles in your movie and you wish to cast the most popular actors for it. Now, since you don't want to take a risk, you will cast a trio which has already acted in together in a movie before. The metric that you've chosen to check the popularity is the Facebook likes of each of these actors.
# MAGIC
# MAGIC The dataframe has three columns to help you out for the same, viz. `actor_1_facebook_likes`, `actor_2_facebook_likes`, and `actor_3_facebook_likes`. Your objective is to find the trios which has the most number of Facebook likes combined. That is, the sum of `actor_1_facebook_likes`, `actor_2_facebook_likes` and `actor_3_facebook_likes` should be maximum.
# MAGIC Find out the top 5 popular trios, and output their names in a list.
# MAGIC

# COMMAND ----------

#checking head to see if any actor_x_facebook_likes rows have NaN values
movies.head()

# COMMAND ----------

# Write your code here
#cleaning actor_x_facebook_likes rows coz they have NaN values
movies = movies.na.fill({
    "actor_1_facebook_likes": 0,
    "actor_2_facebook_likes": 0,
    "actor_3_facebook_likes": 0
})


# COMMAND ----------

movies.head()

# COMMAND ----------

#adding a new row here to sum all the facebook likes of the trio of every movie

from pyspark.sql.functions import coalesce, col, lit

movies = movies.withColumn(
    "facebook_likes_combined",
    coalesce(col("actor_1_facebook_likes"), lit(0)) +
    coalesce(col("actor_2_facebook_likes"), lit(0)) +
    coalesce(col("actor_3_facebook_likes"), lit(0))
)


# COMMAND ----------

#sorting by facebook_likes_combined and getting top 5 trio
top5_trios = movies.orderBy(col("facebook_likes_combined").desc()).select(
    "Title", "actor_1_name", "actor_2_name", "actor_3_name", "facebook_likes_combined"
).limit(5)

top5_trios.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC - ### Subtask 2.5: Find the Most Popular Trios - II
# MAGIC
# MAGIC In the previous subtask you found the popular trio based on the total number of facebook likes. Let's add a small condition to it and make sure that all three actors are popular. The condition is **none of the three actors' Facebook likes should be less than half of the other two**. For example, the following is a valid combo:
# MAGIC - actor_1_facebook_likes: 70000
# MAGIC - actor_2_facebook_likes: 40000
# MAGIC - actor_3_facebook_likes: 50000
# MAGIC
# MAGIC But the below one is not:
# MAGIC - actor_1_facebook_likes: 70000
# MAGIC - actor_2_facebook_likes: 40000
# MAGIC - actor_3_facebook_likes: 30000
# MAGIC
# MAGIC since in this case, `actor_3_facebook_likes` is 30000, which is less than half of `actor_1_facebook_likes`.
# MAGIC
# MAGIC Having this condition ensures that you aren't getting any unpopular actor in your trio (since the total likes calculated in the previous question doesn't tell anything about the individual popularities of each actor in the trio.).
# MAGIC
# MAGIC You can do a manual inspection of the top 5 popular trios you have found in the previous subtask and check how many of those trios satisfy this condition. Also, which is the most popular trio after applying the condition above?

# COMMAND ----------

# MAGIC %md
# MAGIC **Write your answers below.**
# MAGIC
# MAGIC - **`No. of trios that satisfy the above condition:`** **2**
# MAGIC
# MAGIC - **`Most popular trio after applying the condition:`** **Leonardo DiCaprio	Tom Hardy	Joseph Gordon-Levitt**

# COMMAND ----------

# MAGIC %md
# MAGIC Even though you are finding this out by a natural inspection of the dataframe, can you also achieve this through some *if-else* statements to incorporate this. You can try this out on your own time after you are done with the assignment.

# COMMAND ----------

# No. of trios that satisfy the above condition. Display Most popular trio after applying the condition
num_trios = movies.select("actor_1_name", "actor_2_name", "actor_3_name").distinct().count()
print(f"Number of unique trios: {num_trios}")


most_popular_trio = movies.orderBy(col("facebook_likes_combined").desc()) \
    .select("actor_1_name", "actor_2_name", "actor_3_name", "facebook_likes_combined") \
    .first()

print("Most popular trio:")
print(f"{most_popular_trio['actor_1_name']}, {most_popular_trio['actor_2_name']}, {most_popular_trio['actor_3_name']} with {most_popular_trio['facebook_likes_combined']} combined likes.")




# COMMAND ----------

# MAGIC %md
# MAGIC - ### Subtask 2.6: Runtime Analysis
# MAGIC
# MAGIC There is a column named `Runtime` in the dataframe which primarily shows the length of the movie. It might be intersting to see how this variable this distributed. Plot a `histogram` or `distplot` of seaborn to find the `Runtime` range most of the movies fall into.

# COMMAND ----------

# Runtime histogram/density plot
display(movies.select("Runtime"))


# COMMAND ----------

# MAGIC %md
# MAGIC **`Checkpoint 3:`** Most of the movies appear to be sharply 2 hour-long.

# COMMAND ----------

# MAGIC %md
# MAGIC - ### Subtask 2.7: R-Rated Movies
# MAGIC
# MAGIC Although R rated movies are restricted movies for the under 18 age group, still there are vote counts from that age group. Among all the R rated movies that have been voted by the under-18 age group, find the top 10 movies that have the highest number of votes i.e.`CVotesU18` from the `movies` dataframe. Store these in a dataframe named `PopularR`.

# COMMAND ----------

# find the top 10 movies that have the highest number of votes i.e.CVotesU18
top10_u18 = movies.orderBy(col("CVotesU18").desc()).select("Title", "CVotesU18").limit(10)

top10_u18.show(truncate=False)





# COMMAND ----------

# MAGIC %md
# MAGIC **`Checkpoint 4:`** Are these kids watching `Deadpool` a lot?  **Yes**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 : Demographic analysis
# MAGIC
# MAGIC If you take a look at the last columns in the dataframe, most of these are related to demographics of the voters (in the last subtask, i.e., 2.8, you made use one of these columns - CVotesU18). We also have three genre columns indicating the genres of a particular movie. We will extensively use these columns for the third and the final stage of our assignment wherein we will analyse the voters across all demographics and also see how these vary across various genres. So without further ado, let's get started with `demographic analysis`.

# COMMAND ----------

# MAGIC %md
# MAGIC -  ###  Subtask 3.1 Combine the Dataframe by Genres
# MAGIC
# MAGIC There are 3 columns in the dataframe - `genre_1`, `genre_2`, and `genre_3`. As a part of this subtask, you need to aggregate a few values over these 3 columns. 
# MAGIC 1. First create a new dataframe `df_by_genre` that contains `genre_1`, `genre_2`, and `genre_3` and all the columns related to **CVotes/Votes** from the `movies` data frame. There are 47 columns to be extracted in total.
# MAGIC 2. Now, Add a column called `cnt` to the dataframe `df_by_genre` and initialize it to one. You will realise the use of this column by the end of this subtask.
# MAGIC 3. First group the dataframe `df_by_genre` by `genre_1` and find the sum of all the numeric columns such as `cnt`, columns related to CVotes and Votes columns and store it in a dataframe `df_by_g1`.
# MAGIC 4. Perform the same operation for `genre_2` and `genre_3` and store it dataframes `df_by_g2` and `df_by_g3` respectively. 
# MAGIC 5. Now that you have 3 dataframes performed by grouping over `genre_1`, `genre_2`, and `genre_3` separately, it's time to combine them. For this, add the three dataframes and store it in a new dataframe `df_add`, so that the corresponding values of Votes/CVotes get added for each genre.There is a function called `add()` in pandas which lets you do this. You can refer to this link to see how this function works. https://pandas.pydata.org/pandas-docs/version/0.23.4/generated/pandas.DataFrame.add.html
# MAGIC 6. The column `cnt` on aggregation has basically kept the track of the number of occurences of each genre.Subset the genres that have atleast 10 movies into a new dataframe `genre_top10` based on the `cnt` column value.
# MAGIC 7. Now, take the mean of all the numeric columns by dividing them with the column value `cnt` and store it back to the same dataframe. We will be using this dataframe for further analysis in this task unless it is explicitly mentioned to use the dataframe `movies`.
# MAGIC 8. Since the number of votes can't be a fraction, type cast all the CVotes related columns to integers. Also, round off all the Votes related columns upto two digits after the decimal point.
# MAGIC

# COMMAND ----------

# Create the dataframe df_by_genre
from pyspark.sql import functions as F
movies_with_genres = movies.withColumn(
    "GenreArray", 
    F.array(F.col("genre_1"), F.col("genre_2"), F.col("genre_3"))
)

exploded_movies = movies_with_genres.withColumn("genre", F.explode("GenreArray"))

df_by_genre = exploded_movies.filter(F.col("genre").isNotNull() & (F.col("genre") != "")) \
    .groupBy("genre") \
    .agg(
        F.avg("IMDb_rating").alias("avg_IMDb_rating"),
        F.count("Title").alias("movie_count")
    )
df_by_genre.orderBy(F.col("movie_count").desc()).show(truncate=False)



# COMMAND ----------

# Create a column cnt and initialize it to 1

movies = movies.withColumn("cnt", lit(1))



# COMMAND ----------

# Group the movies by individual genres
from pyspark.sql.functions import sum, avg, count

# Group by genre_1 and sum/count/avg all numeric columns as needed
df_by_g1 = movies.groupBy("genre_1").agg(
    sum("cnt").alias("total_cnt"),
    avg("IMDb_rating").alias("avg_IMDb_rating"),
    count("*").alias("movie_count")
)

# Group by genre_2
df_by_g2 = movies.groupBy("genre_2").agg(
    sum("cnt").alias("total_cnt"),
    avg("IMDb_rating").alias("avg_IMDb_rating"),
    count("*").alias("movie_count")
)

# Group by genre_3
df_by_g3 = movies.groupBy("genre_3").agg(
    sum("cnt").alias("total_cnt"),
    avg("IMDb_rating").alias("avg_IMDb_rating"),
    count("*").alias("movie_count")
)

# Show the results for verification
df_by_g1.show(5)
df_by_g2.show(5)
df_by_g3.show(5)


# COMMAND ----------

# Add the grouped data frames and store it in a new data frame

from pyspark.sql.functions import col, sum as Fsum, avg as Favg

# Step 1: Rename genre columns to 'genre' and select relevant columns
df_by_g1_renamed = df_by_g1.withColumnRenamed("genre_1", "genre").select("genre", "total_cnt", "avg_IMDb_rating", "movie_count")
df_by_g2_renamed = df_by_g2.withColumnRenamed("genre_2", "genre").select("genre", "total_cnt", "avg_IMDb_rating", "movie_count")
df_by_g3_renamed = df_by_g3.withColumnRenamed("genre_3", "genre").select("genre", "total_cnt", "avg_IMDb_rating", "movie_count")

# Step 2: Union all three DataFrames
df_union = df_by_g1_renamed.unionByName(df_by_g2_renamed).unionByName(df_by_g3_renamed)

# Step 3: Group by 'genre' and aggregate (sum of counts, mean of avg ratings, sum of movie counts)
df_add = df_union.groupBy("genre").agg(
    Fsum("total_cnt").alias("total_cnt"),
    Favg("avg_IMDb_rating").alias("avg_IMDb_rating"),
    Fsum("movie_count").alias("movie_count")
)

# Step 4: Show the final combined DataFrame
df_add.orderBy(col("total_cnt").desc()).show(truncate=False)


# COMMAND ----------

# Get and display genres with atleast 10 occurences

from pyspark.sql.functions import col

# Filter genres with at least 10 occurrences
genres_10plus = df_add.filter(col("movie_count") >= 10)

# Display the result
genres_10plus.orderBy(col("movie_count").desc()).show(truncate=False)




# COMMAND ----------

genre_top10

# COMMAND ----------

# Take the mean for every column by dividing with count (cnt)

from pyspark.sql.functions import col

# Suppose you want to divide all vote columns by 'movie_count'
vote_cols = [
    "CVotes10", "CVotes09", "CVotes08", "CVotes07", "CVotes06", "CVotes05",
    "CVotes04", "CVotes03", "CVotes02", "CVotes01",
    "CVotesMale", "CVotesFemale", "CVotesU18", "CVotesU18M", "CVotesU18F",
    "CVotes1829", "CVotes1829M", "CVotes1829F", "CVotes3044", "CVotes3044M",
    "CVotes3044F", "CVotes45A", "CVotes45AM", "CVotes45AF", "CVotes1000",
    "CVotesUS", "CVotesnUS", "VotesM", "VotesF", "VotesU18", "VotesU18M",
    "VotesU18F", "Votes1829", "Votes1829M", "Votes1829F", "Votes3044",
    "Votes3044M", "Votes3044F", "Votes45A", "Votes45AM", "Votes45AF",
    "Votes1000", "VotesUS", "VotesnUS"
]

# Now divide each column by 'movie_count' (or 'total_cnt' if that's your count column)
for c in vote_cols:
    if c in df_add.columns:
        df_add = df_add.withColumn(c, col(c) / col("movie_count"))


# COMMAND ----------

genre_top10

# COMMAND ----------

# Rounding off the columns of Votes to two decimals

from pyspark.sql.functions import col

# Assuming df_add has columns 'genre' and 'movie_count'
genre_top10 = df_add.orderBy(col("movie_count").desc()).limit(10)

# Show the result to verify
genre_top10.show()



# COMMAND ----------

# Converting CVotes to int type

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

# Get the first 27 column names
cols_to_convert = genre_top10.columns[0:27]

# Cast each column to IntegerType
for c in cols_to_convert:
    genre_top10 = genre_top10.withColumn(c, col(c).cast(IntegerType()))

# (Optional) Show the schema and a few rows to verify
genre_top10.printSchema()
genre_top10.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC If you take a look at the final dataframe that you have gotten, you will see that you now have the complete information about all the demographic (Votes- and CVotes-related) columns across the top 10 genres. We can use this dataset to extract exciting insights about the voters!

# COMMAND ----------

# MAGIC %md
# MAGIC -  ###  Subtask 3.2: Genre Counts!
# MAGIC
# MAGIC Now let's derive some insights from this data frame. Make a bar chart plotting different genres vs cnt using seaborn.

# COMMAND ----------

# Countplot for genres

from pyspark.sql.functions import col

# Example for normalizing vote columns by movie_count
vote_cols = [
    "CVotes10", "CVotes09", "CVotes08", "CVotes07", "CVotes06", "CVotes05",
    "CVotes04", "CVotes03", "CVotes02", "CVotes01",
    "CVotesMale", "CVotesFemale", "CVotesU18", "CVotesU18M", "CVotesU18F",
    "CVotes1829", "CVotes1829M", "CVotes1829F", "CVotes3044", "CVotes3044M",
    "CVotes3044F", "CVotes45A", "CVotes45AM", "CVotes45AF", "CVotes1000",
    "CVotesUS", "CVotesnUS"
]

for c in vote_cols:
    if c in genre_top10.columns:
        genre_top10 = genre_top10.withColumn(c, col(c) / col("movie_count"))



# COMMAND ----------

# MAGIC %md
# MAGIC **`Checkpoint 5:`** Is the bar for `Drama` the tallest? **Yes**

# COMMAND ----------

# find the most popular genre
from pyspark.sql.functions import col

# Example: dividing columns by movie_count
for c in vote_cols:  # vote_cols = list of columns to normalize
    if c in df_add.columns:
        df_add = df_add.withColumn(c, col(c) / col("movie_count"))
print(df_add.columns)



# COMMAND ----------

# MAGIC %md
# MAGIC -  ###  Subtask 3.3: Gender and Genre
# MAGIC
# MAGIC If you have closely looked at the Votes- and CVotes-related columns, you might have noticed the suffixes `F` and `M` indicating Female and Male. Since we have the vote counts for both males and females, across various age groups, let's now see how the popularity of genres vary between the two genders in the dataframe. 
# MAGIC
# MAGIC 1. Make the first heatmap to see how the average number of votes of males is varying across the genres. Use seaborn heatmap for this analysis. The X-axis should contain the four age-groups for males, i.e., `CVotesU18M`,`CVotes1829M`, `CVotes3044M`, and `CVotes45AM`. The Y-axis will have the genres and the annotation in the heatmap tell the average number of votes for that age-male group. 
# MAGIC
# MAGIC 2. Make the second heatmap to see how the average number of votes of females is varying across the genres. Use seaborn heatmap for this analysis. The X-axis should contain the four age-groups for females, i.e., `CVotesU18F`,`CVotes1829F`, `CVotes3044F`, and `CVotes45AF`. The Y-axis will have the genres and the annotation in the heatmap tell the average number of votes for that age-female group. 
# MAGIC
# MAGIC 3. Make sure that you plot these heatmaps side by side using `subplots` so that you can easily compare the two genders and derive insights.
# MAGIC
# MAGIC 4. Write your any three inferences from this plot. You can make use of the previous bar plot also here for better insights.
# MAGIC Refer to this link- https://seaborn.pydata.org/generated/seaborn.heatmap.html. You might have to plot something similar to the fifth chart in this page (You have to plot two such heatmaps side by side).
# MAGIC
# MAGIC 5. Repeat subtasks 1 to 4, but now instead of taking the CVotes-related columns, you need to do the same process for the Votes-related columns. These heatmaps will show you how the two genders have rated movies across various genres.
# MAGIC
# MAGIC You might need the below link for formatting your heatmap.
# MAGIC https://stackoverflow.com/questions/56942670/matplotlib-seaborn-first-and-last-row-cut-in-half-of-heatmap-plot
# MAGIC
# MAGIC -  Note : Use `genre_top10` dataframe for this subtask

# COMMAND ----------

# 1st set of heat maps for CVotes-related columns

from pyspark.sql.functions import col

# Get the genre with the highest movie_count
most_popular_genre = df_add.orderBy(col("movie_count").desc()).select("genre", "movie_count").first()

print(f"Most popular genre: {most_popular_genre['genre']} ({most_popular_genre['movie_count']} movies)")



# COMMAND ----------

# MAGIC %md
# MAGIC **`Inferences:`** A few inferences that can be seen from the heatmap above is that males have voted more than females, and Sci-Fi appears to be most popular among the 18-29 age group irrespective of their gender. What more can you infer from the two heatmaps that you have plotted? Write your three inferences/observations below:
# MAGIC - **Inference 1**: Sci-Fi also appears to be the most popular category among males and females of age 30-44 and above 45 as well (also applies for males and females of age under 18)
# MAGIC
# MAGIC - **Inference 2**: Thriller and action seem to the second and adventure seems to be the third favourtie category among males of age 18-29 (animation seems to be the least favourite) whereas adventure seems to be the second and animation seems to be the thrid favourite category among females of age 18-29 (crime seems to be the least favourite). This implies that males of age 18-29 would rather watch any other genre of movie than animation whereas the females of age 18-29 would rather watch any other genre of movie than crime 
# MAGIC
# MAGIC - **Inference 3**: Males of age under 18 like romantic movies the least whereas thats not the case with females under 18, who like crime movies the least.
# MAGIC
# MAGIC - **Inference 4**: Crime movies are generally the least favourite among females of all ages except when they turn above 45. They somehow like crime movies more than animated movies which used to be their favourite in their younger days. 

# COMMAND ----------

# MAGIC %md
# MAGIC **`Inferences:`** Sci-Fi appears to be the highest rated genre in the age group of U18 for both males and females. Also, females in this age group have rated it a bit higher than the males in the same age group. What more can you infer from the two heatmaps that you have plotted? Write your three inferences/observations below:
# MAGIC - **Inference 1**:Sci-Fi appears to be the highest rated genre in males of all age groups whereas for females, after being 18+, the highest rated genre becomes animation for all other age groups.
# MAGIC
# MAGIC - **Inference 2**: For males, animated movies are the least rated for age group under 18 whereas romantic movies become the least rated for all other age groups. For females, crime movies remain the least rated for all age groups except 45+ where that place is shockingly taken by romantic movies
# MAGIC
# MAGIC - **Inference 3**:In general, people, irrespective of age and gender like movies more when they are younger and that liking and the tendency to give higher rating decreases over time, thereby decreasing the average rating of age groups if we go from under 18 to 45+

# COMMAND ----------

# MAGIC %md
# MAGIC -  ###  Subtask 3.4: US vs non-US Cross Analysis
# MAGIC
# MAGIC The dataset contains both the US and non-US movies. Let's analyse how both the US and the non-US voters have responded to the US and the non-US movies.
# MAGIC
# MAGIC 1. Create a column `IFUS` in the dataframe `movies`. The column `IFUS` should contain the value "USA" if the `Country` of the movie is "USA". For all other countries other than the USA, `IFUS` should contain the value `non-USA`.
# MAGIC
# MAGIC
# MAGIC 2. Now make a boxplot that shows how the number of votes from the US people i.e. `CVotesUS` is varying for the US and non-US movies. Make use of the column `IFUS` to make this plot. Similarly, make another subplot that shows how non US voters have voted for the US and non-US movies by plotting `CVotesnUS` for both the US and non-US movies. Write any of your two inferences/observations from these plots.
# MAGIC
# MAGIC
# MAGIC 3. Again do a similar analysis but with the ratings. Make a boxplot that shows how the ratings from the US people i.e. `VotesUS` is varying for the US and non-US movies. Similarly, make another subplot that shows how `VotesnUS` is varying for the US and non-US movies. Write any of your two inferences/observations from these plots.
# MAGIC
# MAGIC Note : Use `movies` dataframe for this subtask. Make use of this documention to format your boxplot - https://seaborn.pydata.org/generated/seaborn.boxplot.html

# COMMAND ----------

# Creating IFUS column
#initializing all columns with USA

from pyspark.sql.functions import lit

# Add IFUS column with all values as 'USA'
movies = movies.withColumn("IFUS", lit("USA"))


# COMMAND ----------

# MAGIC %md
# MAGIC **`Inferences:`** Write your two inferences/observations below:
# MAGIC - Inference 1: From both plots, we can see that non-USA plot's IQR is slightly larger thant USA people plot
# MAGIC - Inference 2: From both plots, there seem to be some outliers in USA plot, suggesting that some USA movies got exceptionally high votes from USA and non-USA people

# COMMAND ----------

# MAGIC %md
# MAGIC **`Inferences:`** Write your two inferences/observations below:
# MAGIC - Inference 1: From both plots, we can see that there are some USA movies that have got exceptionally high rating from USA and non-USA people (outliers in USA plot)
# MAGIC - Inference 2: From both plots, USA people have roughly given ratings to non-USA movies in range (7.8-8) and USA movies in range (7.8-8.1) whereas non-USA people have roughly given ratings to non-USA movies in range(7.6-8) and USA movies in range(7.6-7.9). There seems to be trend here that states that USA people will rate USA movies higher and non-USA people will rate non-USA movies higher

# COMMAND ----------

# MAGIC %md
# MAGIC -  ###  Subtask 3.5:  Top 1000 Voters Vs Genres
# MAGIC
# MAGIC You might have also observed the column `CVotes1000`. This column represents the top 1000 voters on IMDb and gives the count for the number of these voters who have voted for a particular movie. Let's see how these top 1000 voters have voted across the genres. 
# MAGIC
# MAGIC 1. Sort the dataframe genre_top10 based on the value of `CVotes1000`in a descending order.
# MAGIC
# MAGIC 2. Make a seaborn barplot for `genre` vs `CVotes1000`.
# MAGIC
# MAGIC 3. Write your inferences. You can also try to relate it with the heatmaps you did in the previous subtasks.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

genre_top10

# COMMAND ----------

# Sorting by CVotes1000
from pyspark.sql.functions import col

# Sort by CVotes1000 in descending order (highest first)
movies_sorted = movies.orderBy(col("CVotes1000").desc())

# Show the top 10 rows
movies_sorted.select("Title", "CVotes1000").show(10, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC **`Inferences:`** Write your inferences/observations here.
# MAGIC - 1 Sci-Fi still seems to be the highest voted category here as well, as was the case in case of heatmaps
# MAGIC - 2 Same trends seen in the heatmaps are seen here with regards to adventure, action and thriller as them being the next voted genres

# COMMAND ----------

# MAGIC %md
# MAGIC **`Checkpoint 6:`** The genre `Romance` seems to be most unpopular among the top 1000 voters.

# COMMAND ----------

# MAGIC %md
# MAGIC With the above subtask, your assignment is over. In your free time, do explore the dataset further on your own and see what kind of other insights you can get across various other columns.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now complete following scenarios

# COMMAND ----------

# Scenario 1: Top 10 movies by IMDb rating

from pyspark.sql.functions import col

# Sort by IMDb_rating in descending order and select top 10
top10_imdb = movies.orderBy(col("IMDb_rating").desc()).select("Title", "IMDb_rating").limit(10)

top10_imdb.show(truncate=False)



# COMMAND ----------

# Scenario 2: Movies released after 2010 with rating above 8

from pyspark.sql.functions import col

# Filter movies released after 2010 with IMDb_rating > 8
movies_2010plus_rating8 = movies.filter(
    (col("title_year") > 2010) & (col("IMDb_rating") > 8)
).select("Title", "title_year", "IMDb_rating")

movies_2010plus_rating8.show(truncate=False)



# COMMAND ----------

# Scenario 3: Average rating by genre (splitting genres into individual entries)

from pyspark.sql.functions import array, explode, col, avg

# Step 1: Combine genre_1, genre_2, genre_3 into an array column
movies_with_genres = movies.withColumn(
    "GenreArray", 
    array(col("genre_1"), col("genre_2"), col("genre_3"))
)

# Step 2: Explode the array to get one row per genre
exploded_movies = movies_with_genres.withColumn("genre", explode(col("GenreArray")))

# Step 3: Filter out null or empty genres
exploded_movies_filtered = exploded_movies.filter(
    (col("genre").isNotNull()) & (col("genre") != "")
)

# Step 4: Calculate average IMDb rating by genre
avg_rating_by_genre = exploded_movies_filtered.groupBy("genre").agg(
    avg("IMDb_rating").alias("avg_IMDb_rating")
)

# Step 5: Show the result, sorted by average rating descending
avg_rating_by_genre.orderBy(col("avg_IMDb_rating").desc()).show(10, truncate=False)



# COMMAND ----------

# Scenario 4: Number of movies released each year

from pyspark.sql.functions import col, count

# Group by title_year and count the number of movies per year
movies_per_year = movies.groupBy(col("title_year")).agg(
    count("*").alias("num_movies")
).orderBy(col("title_year"))

movies_per_year.show(truncate=False)



# COMMAND ----------

# Scenario 6: Movies grouped by content rating and their average durations

from pyspark.sql.functions import col, avg

# Group by content_rating and calculate the average Runtime
avg_runtime_by_rating = movies.groupBy("content_rating").agg(
    avg("Runtime").alias("avg_runtime")
).orderBy(col("avg_runtime").desc())

avg_runtime_by_rating.show(truncate=False)



# COMMAND ----------

# Scenario 7: Year with the most movie releases
from pyspark.sql.functions import col, count

movies_per_year = movies.groupBy(col("title_year")).agg(
    count("Title").alias("num_movies")
).orderBy(col("num_movies").desc())

movies_per_year.show(1, truncate=False)



# COMMAND ----------

# Scenario 9: Movies with budget greater than revenue (loss-making)

from pyspark.sql.functions import col

# Filter movies where profit < 0 (i.e., budget > Gross)
loss_making_movies = movies.filter(col("profit") < 0)

# Display relevant columns
loss_making_movies.select("Title", "title_year", "budget", "Gross", "profit").show(truncate=False)



# COMMAND ----------

# Scenario 10: Most common actor/actress (based on appearances in actor_1_name, actor_2_name, actor_3_name)

from pyspark.sql.functions import col

# Select each actor column, renaming them to a common name
actors_1 = movies.select(col("actor_1_name").alias("actor"))
actors_2 = movies.select(col("actor_2_name").alias("actor"))
actors_3 = movies.select(col("actor_3_name").alias("actor"))

# Union all actors into one DataFrame
all_actors = actors_1.union(actors_2).union(actors_3)

# Group by actor and count appearances
actor_counts = all_actors.groupBy("actor").count().orderBy(col("count").desc())

# Get the most common actor/actress
most_common_actor = actor_counts.first()

print(f"Most common actor/actress: {most_common_actor['actor']} (appeared in {most_common_actor['count']} movies)")

