import os
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# Set environment variables for Spark and Java
os.environ['JAVA_HOME'] = '/opt/bitnami/java'
os.environ['SPARK_HOME'] = '/opt/bitnami/spark'

# Create a Spark session
spark = SparkSession.builder.appName("Stock Price Prediction").getOrCreate()

# Load the dataset from HDFS
data_path = "hdfs://namenode:8020/input/stock_data_IBM.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)

df.show(5)

# Data preparation
assembler = VectorAssembler(
    inputCols=["Open", "High", "Low", "Volume"],  # Features
    outputCol="features"
)

df = assembler.transform(df)

# Select relevant columns (features and target)
df = df.select("features", "Close")

# Split data into training and test sets
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# Train a Random Forest Regression model
rf = RandomForestRegressor(featuresCol="features", labelCol="Close")

# Hyperparameter tuning (example using CrossValidator)
param_grid = (ParamGridBuilder()
              .addGrid(rf.numTrees, [50, 100])
              .addGrid(rf.maxDepth, [5, 10])
              .build())

evaluator = RegressionEvaluator(labelCol="Close", predictionCol="prediction", metricName="rmse")
cv = CrossValidator(estimator=rf, estimatorParamMaps=param_grid, evaluator=evaluator, numFolds=3)

# Fit the model using CrossValidator
model = cv.fit(train_df)

# Evaluate the model
predictions = model.transform(test_df)
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE): {rmse:.2f}")

# Save the trained model to HDFS
model.bestModel.write().overwrite().save("hdfs://namenode:8020/stock_price_model")

# Stop the Spark session
spark.stop()
