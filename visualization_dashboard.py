import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession

# Initialize PySpark Session
spark = SparkSession.builder.appName("Stock Data Visualization").getOrCreate()

# HDFS data path
data_path = "hdfs://namenode:8020/input/stock_data_IBM.csv"

# Set up page configuration
st.set_page_config(page_title="Stock Data Visualization", layout="wide")

# Title of the dashboard
st.title("Stock Data Visualization")

# Try loading the dataset
try:
    # Read the CSV file into a Spark DataFrame
    df_spark = spark.read.csv(data_path, header=True, inferSchema=True)
    df = df_spark.toPandas()  # Convert Spark DataFrame to Pandas DataFrame

    # Display basic info about the data
    st.write("### Data Preview")
    st.write(df.head())

    # Display basic statistics about the dataset
    st.write("### Data Statistics")
    st.write(df.describe())

    # Ensure the 'Date' column is in datetime format for accurate plotting
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])

    # Visualizing data
    st.write("### Line Chart of Stock Prices Over Time")
    fig, ax = plt.subplots(figsize=(6, 3))
    ax.plot(df['Date'], df['Close'], label="Close Price", color='b')
    ax.set_title('Stock Close Price Over Time', fontsize=12)  # Reduced font size
    ax.set_xlabel('Date', fontsize=10)  # Reduced font size
    ax.set_ylabel('Price', fontsize=10)  # Reduced font size
    ax.legend(fontsize=9)  # Reduced legend font size
    st.pyplot(fig)

    # Add a histogram for volume
    st.write("### Histogram of Trading Volume")
    fig, ax = plt.subplots(figsize=(6, 3))
    ax.hist(df['Volume'], bins=50, color='g', edgecolor='black')
    ax.set_title('Trading Volume Distribution', fontsize=12)  # Reduced font size
    ax.set_xlabel('Volume', fontsize=10)  # Reduced font size
    ax.set_ylabel('Frequency', fontsize=10)  # Reduced font size
    st.pyplot(fig)

    # Box plot for stock prices
    st.write("### Box Plot of Stock Prices")
    fig, ax = plt.subplots(figsize=(6, 3))
    sns.boxplot(x=df['Close'], ax=ax, color='r')
    ax.set_title('Stock Price Distribution', fontsize=12)  # Reduced font size
    ax.set_xlabel('Close Price', fontsize=10)  # Reduced font size
    st.pyplot(fig)

    # Stop Spark session
    spark.stop()

except Exception as e:
    st.error(f"An error occurred while loading the data: {e}")
