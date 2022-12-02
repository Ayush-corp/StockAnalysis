import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, format_number,mean,max,min,count,year

S3_DATA_SOURCE_PATH = 's3://stock-analysis-emr-bucket/data-source/Stocks/'
S3_DATA_OUTPUT_PATH = 's3://stock-analysis-emr-bucket/data-output'

s3 = boto3.client('s3')
response = s3.list_objects_v2(
    Bucket = 'stock-analysis-emr-bucket',
    Prefix = 'data-source/Stocks/'
)
spark = SparkSession.builder.appName('StockAnalysis').getOrCreate()
columns=["Stock Name","max(High)","min(High)","max(Low)","min(Low)","avg(close)","max(Volume)","min(Volume)"]
payload = []
count = 0

for i in response['Contents']:
    j = i['Key']
    all_data = spark.read.csv(f"{S3_DATA_SOURCE_PATH}/{j}", header=True)
    file_name = j[:j.index('.')]
    try:
        avg_close = all_data.select(mean("Close")).collect()[0][0]
        max_high = all_data.select(max("High")).collect()[0][0]
        max_volume = all_data.select(max("Volume")).collect()[0][0]
        max_low = all_data.select(max("Low")).collect()[0][0]
        min_high = all_data.select(min("High")).collect()[0][0]
        min_volume = all_data.select(min("Volume")).collect()[0][0]
        min_low = all_data.select(min("Low")).collect()[0][0]
    except:
        avg_close,max_high,max_volume,max_low,min_high,min_volume,min_low = 0,0,0,0,0,0,0
    count += 1
    
    data = (str(file_name),str(max_high),str(min_high),str(max_low),str(min_low),str(avg_close),str(max_volume),str(min_volume))
    payload.append(data)
    df = spark.createDataFrame(data = payload, schema = columns)
    df.write.mode('overwrite').csv(S3_DATA_OUTPUT_PATH)
    print(f"Successfully analysed {count} Stocks")
    
