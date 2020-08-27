from pyspark import SparkSession # import spark session
spark = SparkSession.builder.appName("myapp").getOrCreate() # create a spark session
df = spark.json('people.json') # upload the data 

# show dataframe

df.show()

# to know schema of dataframe
df.printSchema()

# just want column names

df.columns # this will give list in python 

# get summary of dataframe

df.describe()

# to show summary along with numerical values
# count , mean,std deviation, min , max

df.describe().show()

# schema needs to be correct and clarified , like tell which is row, which is column
# i.e you want to tell the structure 

# so first import the type tools.

from pyspark.sql.types import (StructField,StringType,
                                IntegerType,StructType)


# now create a data schema so that when you dont have structured data
# you can create your own structured schema

# create a list
data_schema = [StructField('age',IntegerType(),True),
StructField('name',StringType(),True)]

# so here created a structure field having name as type of string  and age as type ineteger.
# True because if it has null values it should not give error.

# now after creating schema create a structure for this.

final_structure = StructType(fileds=data_schema) # this gives final structure and also required is fields value.


# to display the schema now

df = spark.read.json('people.json', schema=final_structure) # this will read the data and also give in the schema that we just created.
df.printSchema # this prints the new schema that we created before.

# ow this is how to grab the data i.e selecting the data.

# 'df' is a column type of object 

# to get dataframe as object use

df.select('value') # df.select('age) # this will give dataframe .

# so we will use select more.

# to get the rows in table,

df.head(2) # 2 is number of rows you want 
df.head(2)[0] # get the first row out of 2.

# to show multiple columns or some features

df.select(['age','name']).show()

# create a column ,
# all these operations are not inplace meaning they are not permanent
# to make them permanent save them to a new variable.

df,withColumn('newnameofcolumn',df['age']).show()

# here to add one more column with name 'newnameofcolumn' with the age column type above command.

# can you use pure SQL to deal with dataframe ?

# YES 


# first create a temporary view to store the data , here data = people.

df.cerateOrReplaceTempView('people')

# now store this in some new variable


results  = spark.sql("select * from people") # this shows how to run a pure SQL query.

results.show() # this will output the results.

# now to modify query

new_results = spark.sql("select * from people where age > 20")
new_results.show()


# load a new data file maybe csv 

df = spark.read.csv('name.csv',inferSchema=True, header=True) 
# inferschema will automatically create data type of the data inside the table or data set.

df.printSchema()

df.show()

# now suppose you want to apply some sql filters 

df.filter("condition < value").select('somecolumn','somecolumn2').show()

# suppose you want to apply multiple rules or filters

df.filter ( (df["condition"] < value) & (df["condition"] > value) ).show()

# operators in spark 

# & = and
# ~ = NOT 
# | = OR


# to get specific information from a row.

row.asDict()['what do you want here?']

# this will generate the rows related to whatever you want or have requested.


# groupby and aggregation of dataframes.

# how to group by column names 

# type of data objhect is GroupedData.

df.groupBy("name_of_column").show()
# with above you can show names of columns in the data .

# suppose you want the find the mean or some other metrics.

df.groupBy("name_of_column").mean().show() # this will show the mean of that column which was specified by you.

# mean,min,max,std.deviation,count etc.

# not all data require groupby and mpost of them work well with aggregation. i.e agg call.

# aggregating here
df.agg({'name_of_column' : 'some_operation_like_mean_etc'}).show()

# this is how its similar to groupby.


# How to import functions ?

from pyspark.sql.functions import <name of function>

# to apply functions

df.select(avg('name_here')).show()

# to format decimal numbers use the format_number .

df.select(format_number('name_of_column',<how_many_digits_to_display>)).show()
# this will show us output upto mentioned number of decimals.


# how to use ordering ?

# use ORDERBY 

df.orderBy('name').show()

# to order in specific format like descending 

df.orderBy(df['name'].desc()).show()


# how to handle MISSING data ?

# keep missing points as null values
# drop the missing values
# fill them with some other values.

df.na.drop().show() # this will drop the missing values.

# now suppose you want to have those rows who have atleast 2 NON-NULL values.

df.na.drop(thresh=2).show()

# this will give the rows with atleast 2 non-null values.


# suppose you want to drop the row who has all the null values.

df.na.drop(how='all').show() # this will drop only that row which has all null values.

# now you want to drop the null value from a specific column.

df.na.drop(subset=['name']).show() # always use the subset


# now you want to fill thenull values with some information 

df.na.fill('FILL MISSING VALUES').show() # only for string.

# this will fill the null values with the word = FILL MISSING VALUES


# now to fill in numbers or integers as seen previously for strings.

df.na.fill(0).show() # this will fill '0' as null value in place of null values in integer columns.


# date and time objects.

# you have a dataset 

df.select(['Date','Open']).show()


# use of functions 

from pyspark.sql.functions import (dayofmonth,hour,
dayofyear,month,year,weekofyear,format_number,date_format)

df.select(dayofmonth(df['name_of_column'])).show()

# suppose you want to know average closing price of some commodity 

df.withColumn("Year",year(df['Date'])).show()

# lets name this to something else

newdataframe = df.withColumn("Year",year(df['Date'])).show()

newdataframe.groupBy("Year").mean().select(["Year","avg(Close)"]).show()

# this above will show me the avg closing price of the commodities over the years.


