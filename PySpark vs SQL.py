# Databricks notebook source
# MAGIC %md
# MAGIC # Setting Path for data source

# COMMAND ----------

# Read data
input_path = '/mnt/bronze/SalesLT/Address/Address.parquet'
df = spark.read.format('parquet').load(input_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Selecting Column

# COMMAND ----------

# select column in pyspark
selec_col = df.select("AddressID","CountryRegion")
display(selec_col)

# COMMAND ----------

# Select column in SQL
#First, you need to register your DataFrame as a temporary SQL view so that you can run SQL queries against it.
df.createOrReplaceTempView("Address")

# COMMAND ----------

# Select column in SQL

display(spark.sql("SELECT 'AddressIS', 'CountryRegion' FROM my_table"))


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- directly using Sql Command
# MAGIC select * from Address

# COMMAND ----------

# MAGIC %md
# MAGIC # Filter Rows

# COMMAND ----------

# In Pyspark
display(df.filter("AddressID = 25"))
display(df.select("AddressID","CountryRegion").filter("AddressId = 25"))

# COMMAND ----------

# in pyspark SQL
display(spark.sql("Select * from Address where addressID = 25"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In SQL
# MAGIC select * from Address where addressID = 25

# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregate Functions

# COMMAND ----------

# In PySpark
from pyspark.sql import functions as F
display(df.select(F.avg("AddressID")))
display(df.select(F.sum('AddressID')))
display(df.select(F.max('AddressID')))
display(df.select(F.min("AddressId")))

# COMMAND ----------

# In PySpark SQL
display(spark.sql("SELECT avg(AddressID) FROM Address"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In SQL
# MAGIC Select avg(AddressID) from Address;
# MAGIC Select sum(AddressID) from Address;
# MAGIC select min(AddressID) from Address;
# MAGIC select max(AddressID) from Address;
# MAGIC select count(AddressID) from Address;

# COMMAND ----------

# MAGIC %md
# MAGIC # Group By Function

# COMMAND ----------

# In Pyspark
display(df.groupBy("countryRegion").count())

# COMMAND ----------

# in pySpark SQL

display(spark.sql("Select CountryRegion ,count(CountryRegion) as Total_Country from Address group by CountryRegion"))

# COMMAND ----------

# MAGIC %sql
# MAGIC Select CountryRegion ,count(CountryRegion) as Total_Country from Address group by CountryRegion

# COMMAND ----------

# MAGIC %md
# MAGIC # Order by

# COMMAND ----------

display(df.orderBy("AddressID", ascending = True))

# COMMAND ----------

# In PySpark Sql

display(spark.sql("Select * from Address order by AddressID ASC"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In SQl
# MAGIC Select * from Address order by AddressID ASC

# COMMAND ----------

# MAGIC %md
# MAGIC # JOIN

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze")
input_pathone = "/mnt/bronze/SalesLT/Product/Product.parquet"
input_pathtwo = "/mnt/bronze/SalesLT/ProductCategory/ProductCategory.parquet"


df1 = spark.read.format("parquet").load(input_pathone)
df2 = spark.read.format("parquet").load(input_pathtwo)


# COMMAND ----------

display(df1)

# COMMAND ----------

df1.printSchema()
df2.printSchema()

# COMMAND ----------

# Join in PySpark
df3 = df1.join(df2,df1.ProductCategoryID == df2.ProductCategoryID)

# COMMAND ----------

display(df3)

# COMMAND ----------

df1.createOrReplaceTempView("Product")
df2.createOrReplaceTempView("ProductCategory")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Product join ProductCategory on Product.ProductCategoryID = ProductCategory.ProductCategoryID;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Union
# MAGIC
# MAGIC Key Points:
# MAGIC UNION:
# MAGIC
# MAGIC Combines the results of two or more SELECT statements.
# MAGIC Removes duplicate rows from the result set.
# MAGIC Ensures that all rows in the result set are distinct.
# MAGIC UNION ALL:
# MAGIC
# MAGIC Combines the results of two or more SELECT statements.
# MAGIC Includes all rows, including duplicates.
# MAGIC Is generally faster than UNION because it does not perform the duplicate elimination step.

# COMMAND ----------



df1.union(df2)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from Product
# MAGIC Union 
# MAGIC Select * from ProductCategory

# COMMAND ----------

# MAGIC %md
# MAGIC # Limit

# COMMAND ----------

# In Pyspark
display(df1.limit(10))
display(df2.limit(10))

# COMMAND ----------

# In PySpark Sql
display(spark.sql("select * from Product limit 10"))

display(spark.sql("select * from ProductCategory limit 10"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL
# MAGIC select * from Product limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC # Distint values

# COMMAND ----------

# In Pyspark
display(df1.select("ProductID").distinct())

# COMMAND ----------

# In PySpark SQl

display(spark.sql("Select DISTINCT ProductID from Product"))


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT ProductID from product

# COMMAND ----------

# MAGIC %md
# MAGIC # Adding a New Column

# COMMAND ----------

# In PySpark
from pyspark.sql import functions as F

df4 = df1.withColumn("New_column",F.col("ProductID") + F.col("Size"))
display(df4)


# COMMAND ----------

# spark Sql
display(spark.sql("Select *, (StandardCost + ListPrice) as New_column from Product"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In SQL
# MAGIC Select *, (StandardCost + ListPrice) as New_column from Product

# COMMAND ----------

# MAGIC %md
# MAGIC # Column Alias

# COMMAND ----------

# in Pyspark
display(df1.select(F.col("ProductID").alias ("ProductID_Alias")).limit(5))
display(df1.limit(5))

# COMMAND ----------

display(spark.sql("Select ProductID as ProductID_alias from Product"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select ProductID as ProductID_alias from Product

# COMMAND ----------

# MAGIC %md
# MAGIC # Filtering on Multiple Condition

# COMMAND ----------

# in pySpark
display(df1.filter((F.col("ProductID") <= 740) & (F.col("Color") == "Black") & (F.col("ListPrice") >= 400)))


# COMMAND ----------

display(spark.sql("select * from Product where ProductID <= 740 AND Color == 'Black' AND ListPrice >=400"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Product where ProductID <= 740 AND Color == 'Black' AND ListPrice >=400

# COMMAND ----------

# MAGIC %md
# MAGIC #SubQuery

# COMMAND ----------

filter_for_avg = df1.filter((F.col("ProductID") <= 740) & (F.col("Color") == "Black") & (F.col("ListPrice") >= 400))

display(filter_for_avg.agg(F.avg("StandardCost")))


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (select * from )

# COMMAND ----------

# MAGIC %md
# MAGIC #Rough
# MAGIC -----------------------------------------------------------------------------------------------------------------------------------------------------------

# COMMAND ----------

#rough start
display(df1.limit(20))

# COMMAND ----------

rough_df = df1.select("Name", "Color","StandardCost")
display(rough_df.limit(5))

# COMMAND ----------

display(rough_df.groupBy("color").avg("StandardCost"))

# COMMAND ----------

rough_df.filter("StandardCost > ")

# COMMAND ----------

# MAGIC %md
# MAGIC ##                                                                          END
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC # Between

# COMMAND ----------

display(df1.limit(5))
display(df2.limit(5))

# COMMAND ----------

from pyspark.sql import functions as F

display(df1.filter(F.col("ListPrice").between(1400,1450)))

# COMMAND ----------

df1.createOrReplaceTempView("Product")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from product where ListPrice between 1400 AND 1450

# COMMAND ----------

display(spark.sql('select * from Product where ListPrice between 1400 AND 1450'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Like

# COMMAND ----------

display(df1.filter(F.col("Color").like("Black")))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select * from product where Color like 'Black'

# COMMAND ----------

display(spark.sql("SELECT * FROM product WHERE Color LIKE 'Black' "))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Case When
# MAGIC The CASE WHEN statement is a conditional expression used in SQL and many SQL-like query languages to handle if-then-else logic within queries. It allows you to perform different actions based on specific conditions, similar to how you might use if-else statements in programming languages.

# COMMAND ----------

#from pyspark.sql import functions as F
#display(df1.select(F.When(F.col("Color") == "Black", F.col("ListPrice")).otherwise(F.col("StandardCost"))))

df8 = df1.select(
                F.col("Color"), 
                F.col("ListPrice"),
                F.col("StandardCost"),
                F.when(F.col("Color") == "Black",F.col("ListPrice"))
                .otherwise(F.col("StandardCost"))
                .alias("Price")
                )

display(df8)        



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE 
# MAGIC         WHEN Color = 'Black' THEN ListPrice
# MAGIC         ELSE StandardCost
# MAGIC     END AS AdjustedPrice, Color, StandardCost, ListPrice
# MAGIC FROM Product;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Cast Data Type
# MAGIC Casting refers to the process of converting a value from one data type to another.This can be necessary when you need to perform operations that require a specific data type or when you need to ensure that data is in the appropriate format for storage or analysis.

# COMMAND ----------

# In PySpark
display(df1)
df8 = df1.withColumn("ListCost",F.col('ListPrice').cast("Integer"))
   #=df.select(F.col("ListPrice").cast("Integer")) 
display(df8)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC SELECT *, cast( ListPrice AS INTEGER) from Product 

# COMMAND ----------

display(spark.sql("SELECT *, cast( ListPrice AS INTEGER) from Product"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Count Distint

# COMMAND ----------

display(df1.select(F.countDistinct("color")))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  COUNT(DISTINCT Color) from Product

# COMMAND ----------

# MAGIC %md
# MAGIC #SubString
# MAGIC A substring is a portion of a string extracted based on specified criteria

# COMMAND ----------

display(df1.select(F.substring("name", 15, 35)))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT substring(name, 14,35) from Product

# COMMAND ----------

# MAGIC %md
# MAGIC #Concatenate Columns
# MAGIC Concatenating columns combines the values from multiple columns into a single column
# MAGIC To concatenate two columns with a space between them in PySpark, you can use the concat_ws function. concat_ws stands for "concatenate with separator," and it allows you to specify a separator between the concatenated values.

# COMMAND ----------

display(df1.withColumn("concated_column",F.concat(F.col("Color"),F.col("Size"))))

# COMMAND ----------

display(df1.withColumn("concated_column",F.concat_ws(" / ",F.col("Color"),F.col("Size"))))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT concat_ws("_",Color, Size) AS Concat_new from Product

# COMMAND ----------

# MAGIC %md
# MAGIC # Average Over Partition
# MAGIC Calculating the average over a partition (or window) allows you to compute averages within subsets of your data rather than over the entire dataset.

# COMMAND ----------

from pyspark.sql.window import Window
display(df1.withColumn("AVG", F.avg("ListPrice").over(Window.partitionBy("Color"))))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *, avg(ListPrice) OVER (PARTITION BY Color) AS AVG_New from Product

# COMMAND ----------

# MAGIC %md 
# MAGIC # Sum Over Partition

# COMMAND ----------

display(df1.withColumn("Sum_New", F.sum("ListPrice").over(Window.partitionBy("Color"))))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, sum(ListPrice) OVER (PARTITION BY Color) AS sum_new FROM product WHERE Color = "Red"

# COMMAND ----------

# MAGIC %md
# MAGIC #Lead Function
# MAGIC The LEAD function is used to access the value of a column in a subsequent row relative to the current row. This is particularly useful for tasks such as calculating differences between rows, performing time-series analysis, or creating lagged features for machine learning models.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ProductID,
# MAGIC     StandardCost,
# MAGIC     LEAD(StandardCost, 1) OVER (ORDER BY ProductID) AS LeadResult
# MAGIC FROM
# MAGIC     Product
# MAGIC ORDER BY
# MAGIC     ProductID
# MAGIC LIMIT 5;

# COMMAND ----------

leaddf = df1.withColumn("NewLead", F.lead("StandardCost", 1).over(Window.orderBy("ProductID")))
display(leaddf.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC # Lag Function
# MAGIC The LAG function, like LEAD, is a window function used to access the value of a column in a preceding row relative to the current row. It is useful for comparing a row with previous rows, often used for tasks such as calculating differences or creating time-series features.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   ProductID, 
# MAGIC   StandardCost,
# MAGIC   lag(StandardCost, 1) OVER (ORDER BY ProductID) AS NewLag
# MAGIC FROM Product
# MAGIC LIMIT 5

# COMMAND ----------

lagdf = df1.withColumn("NewLag", F.lag("StandardCost", 1).over(Window.orderBy("ProductID")))
display(lagdf.select("ProductID", "StandardCost", "NewLag").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC #Row Count

# COMMAND ----------

# In PySpark

df1.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In SQL
# MAGIC SELECT count(*) FROM Product

# COMMAND ----------

# MAGIC %md
# MAGIC #Drop Column

# COMMAND ----------

# In PySpark

lagdf = lagdf.drop("NewLag")

display(lagdf)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In Sql
# MAGIC ALTER TABLE Product
# MAGIC DROP Column ThumbNailPhoto;
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename Column

# COMMAND ----------

# In Pyspark
renamedf = df1.withColumnRenamed("StandardCost", "StandardPrice")
display(renamedf)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- In SQl
# MAGIC ALTER TABLE Product RENAME COLUMN StansardCost TO StandardPrice
# MAGIC

# COMMAND ----------


