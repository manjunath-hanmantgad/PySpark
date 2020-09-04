# Binary Customer Churn

""" A marketing agency has many customers that use their service to produce ads for the client/customer websites. They've noticed that they have quite a bit of churn in clients. They basically randomly assign account managers right now, but want you to create a machine learning model that will help predict which customers will churn (stop buying their service) so that they can correctly assign the customers most at risk to churn an account manager. Luckily they have some historical data, can you help them out? Create a classification algorithm that will help classify whether or not a customer churned. Then the company can test this against incoming data for future customers to predict which customers will churn and assign them an account manager.

The data is saved as customer_churn.csv. Here are the fields and their definitions:

Name : Name of the latest contact at Company
Age: Customer Age
Total_Purchase: Total Ads Purchased
Account_Manager: Binary 0=No manager, 1= Account manager assigned
Years: Totaly Years as a customer
Num_sites: Number of websites that use the service.
Onboard_date: Date that the name of the latest contact was onboarded
Location: Client HQ Address
Company: Name of Client Company

 """
 
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('logregconsult').getOrCreate()
data = spark.read.csv('customer_churn.csv',inferSchema=True,header=True)
data.printSchema()

# check data 

data.describe().show()
data.columns

# MLlib 

from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=['Age',
 'Total_Purchase',
 'Account_Manager',
 'Years',
 'Num_Sites'],outputCol='features')
 
output = assembler.transform(data)

final_data = output.select('features','churn')

# Train - test datset split 

train_churn,test_churn = final_data.randomSplit([0.7,0.3])

# Fit the model 

from pyspark.ml.classification import LogisticRegression
lr_churn = LogisticRegression(labelCol='churn')
fitted_churn_model = lr_churn.fit(train_churn)
training_sum = fitted_churn_model.summary
training_sum.predictions.describe().show()


# evaluate results 

from pyspark.ml.evaluation import BinaryClassificationEvaluator
pred_and_labels = fitted_churn_model.evaluate(test_churn)
pred_and_labels.predictions.show()

# using performance metrics - AUC 

churn_eval = BinaryClassificationEvaluator(rawPredictionCol='prediction',labelCol='churn')
auc = churn_eval.evaluate(pred_and_labels.predictions)
auc

# now time to test 

final_lr_model = lr_churn.fit(final_data)
new_customers = spark.read.csv('new_customers.csv',inferSchema=True,header=True)
new_customers.printSchema()
test_new_customers = assembler.transform(new_customers)
test_new_customers.printSchema()
final_results = final_lr_model.transform(test_new_customers)
final_results.select('Company','prediction').show()