import subprocess
from pyspark.sql import SparkSession

# Define Hadoop commands to stage data
hadoop_cmds = [
    'hdfs dfs -mkdir -p /input_data',
    'hdfs dfs -put /local_data.csv /input_data'
]

# Execute Hadoop commands
for cmd in hadoop_cmds:
    subprocess.run(cmd.split())

# Create Spark session
spark = SparkSession.builder.appName('my_etl').getOrCreate()

# Load data from Hadoop
df = spark.read.format('csv').load('hdfs:///input_data/local_data.csv')

# Apply transformations to data
df_transformed = df.selectExpr('_c0 as id', '_c1 as name', '_c2 as age')

# Write transformed data back to Hadoop
df_transformed.write.format('parquet').save('hdfs:///output_data/transformed_data.parquet')

# Stop Spark session
spark.stop()

# Define Hadoop commands to move data to HDFS
hadoop_cmds = [
    'hdfs dfs -mkdir -p /output_data',
    'hdfs dfs -mv /output_data/transformed_data.parquet /output_data/transformed_data'
]

# Execute Hadoop commands
for cmd in hadoop_cmds:
    subprocess.run(cmd.split())
