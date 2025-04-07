from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ReadParquetFromHDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# Define the Parquet file path in HDFS
parquet_file_path = "hdfs://localhost:9000/user/hadoop/raw_employee_data/part-00000-c09f9aeb-371b-42e6-823f-dc0eb3c738a6-c000.snappy.parquet"

# Read the Parquet file into a DataFrame
df = spark.read.parquet(parquet_file_path)

# Show the data
df.show(truncate=False)

# Print Schema
df.printSchema()
