#Recommender Systems 

import pandas as pd
df = pd.read_csv('movielens_ratings.csv')

df.describe().transpose()

df.corr()

import numpy as np
df['mealskew'] = df['movieId'].apply(lambda id: np.nan if id > 31 else id)

df.describe().transpose()

df['meal_name'] = df['mealskew'].map(mealmap)

df.to_csv('Meal_Info.csv',index=False)

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('recconsulting').getOrCreate()


from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS


data = spark.read.csv('Meal_Info.csv',inferSchema=True,header=True)

(training, test) = data.randomSplit([0.8, 0.2])


# Build the recommendation model using ALS on the training data
als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="mealskew", ratingCol="rating")
model = als.fit(training)

# Evaluate the model by computing the RMSE on the test data
predictions = model.transform(test)

predictions.show()

evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))