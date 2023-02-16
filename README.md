# learn_hadoop
I tried to create a simple Hadoop ETL that uses CSV data and outputs a parquet file.

This ETL process is in three steps:
##### 1. Extract data from a CSV file.
##### 2. Transform the data by changing a few columns using selectExpr() that takes in SQL expressions as strings.
##### 3. Load the data to an HDFS warehouse.

