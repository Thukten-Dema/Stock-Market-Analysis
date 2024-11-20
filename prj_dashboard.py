import os
import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.ml.feature import VectorAssembler, StandardScaler

# Set environment variables for Spark and Java
os.environ['JAVA_HOME'] = '/opt/bitnami/java'
os.environ['SPARK_HOME'] = '/opt/bitnami/spark'

# Create a Spark session
spark = SparkSession.builder.appName("Stock Price Prediction Dashboard").getOrCreate()

# Load the pre-trained model from HDFS (ensure model is saved as RandomForestRegressionModel)
model_path = "hdfs://namenode:8020/stock_price_model"
model = RandomForestRegressionModel.load(model_path)

# Streamlit UI for the prediction dashboard
st.title("Stock Price Prediction Dashboard")

# Input fields for user data
open_price = st.number_input("Open Price ($)", min_value=0.0, max_value=10000.0, step=0.01)
high_price = st.number_input("High Price ($)", min_value=0.0, max_value=10000.0, step=0.01)
low_price = st.number_input("Low Price ($)", min_value=0.0, max_value=10000.0, step=0.01)
volume = st.number_input("Volume", min_value=0, step=1)

# Button to trigger prediction
if st.button("Predict"):
    # Prepare user input for prediction
    user_input = pd.DataFrame([[open_price, high_price, low_price, volume]],
                              columns=["Open", "High", "Low", "Volume"])
    
    # Convert to Spark DataFrame
    user_spark_df = spark.createDataFrame(user_input)
    
    # Assemble features into a single column
    assembler = VectorAssembler(
        inputCols=["Open", "High", "Low", "Volume"],
        outputCol="features"
    )
    user_spark_df = assembler.transform(user_spark_df)
    
    # If scaling was used during training, apply the same scaling
    # Assuming you used StandardScaler during training:
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=False)
    user_spark_df = scaler.fit(user_spark_df).transform(user_spark_df)
    
    # Make prediction using the pre-trained model
    prediction = model.transform(user_spark_df)
    predicted_close_price = prediction.select("prediction").collect()[0][0]
    
    # Display the predicted Close price
    st.write(f"Predicted Close Price: ${predicted_close_price:.2f}")

# Stop the Spark session
spark.stop()
