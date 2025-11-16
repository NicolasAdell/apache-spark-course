import os
import random
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, udf
from pyspark.sql.types import StringType


LOG_FILE = "./access_log.txt"

# Create spark session
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("LogStreamSimulator").getOrCreate()


@udf(StringType())
def get_random_log_line():
    try:
        if not os.path.exists(LOG_FILE):
            return None
        
        file_size = os.path.getsize(LOG_FILE)
        if file_size == 0:
            return None
        
        with open(LOG_FILE, "r") as lf:
            while True:
                random_position = random.randint(0, file_size - 1)
                lf.seek(random_position) # Jump to that position
                lf.readline() # Discard partial line
                line = lf.readline().strip()

                if line: # Valid line
                    return line
                
    except Exception as e:
        print(str(e))
        return None
    

rate_df = spark.readStream.format("rate").option("rowPerSecond", 5).load()

access_lines = rate_df.withColumn("value", get_random_log_line())

# Parse out the common log format to a DataFrame
userAgentExp = r'\"[^\"]*\" \"([^\"]+)\"'
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

@udf(StringType())
def extract_host(log_line):
    match = re.search(hostExp, log_line)
    return match.group(1) if match else None

@udf(StringType())
def extract_timestamp(log_line):
    match = re.search(timeExp, log_line)
    return match.group(1) if match else None

@udf(StringType())
def extract_method(log_line):
    match = re.search(generalExp, log_line)
    return match.group(1) if match else None

@udf(StringType())
def extract_endpoint(log_line):
    match = re.search(generalExp, log_line)
    return match.group(2) if match else None

@udf(StringType())
def extract_protocol(log_line):
    match = re.search(generalExp, log_line)
    return match.group(3) if match else None

@udf(StringType())
def extract_status(log_line):
    match = re.search(statusExp, log_line)
    return match.group(1) if match else None

@udf(StringType())
def extract_content_size(log_line):
    match = re.search(contentSizeExp, log_line)
    return match.group(1) if match else None

@udf(StringType())
def extract_user_agent(log_line):
    match = re.search(userAgentExp, log_line)
    return match.group(1) if match else None

spark.udf.register("extract_host", extract_host)
spark.udf.register("extract_timestamp", extract_timestamp)
spark.udf.register("extract_method", extract_method)
spark.udf.register("extract_endpoint", extract_endpoint)
spark.udf.register("extract_protocol", extract_protocol)
spark.udf.register("extract_status", extract_status)
spark.udf.register("extract_content_size", extract_content_size)
spark.udf.register("extract_user_agent", extract_user_agent)

access_lines.createOrReplaceTempView("raw_logs")

structered_logs_query = '''
    SELECT
        extract_host(value) AS host,
        extract_timestamp(value) AS timpestamp,
        extract_method(value) AS method,
        extract_endpoint(value) AS endpoint,
        extract_protocol(value) AS protocol,
        extract_status(value) AS status,
        extract_content_size(value) AS content_size,
        extract_user_agent(value) AS user_agent
    FROM raw_logs
'''

logsDF = spark.sql(structered_logs_query)
# Register logsDF as a table
logsDF.createOrReplaceTempView("access_logs")

topUserAgentsDF = spark.sql('''
    SELECT
        user_agent,
        COUNT(*) AS count
        FROM access_logs
        WHERE user_agent IS NOT NULL
        GROUP BY user_agent
        ORDER BY count DESC
        LIMIT 10          
''')

# Kick off streaming query
query = (topUserAgentsDF.writeStream.outputMode("complete").format("console").queryName("top_user_agents").start())

# Run forever until it's terminated
query.awaitTermination()

spark.stop()