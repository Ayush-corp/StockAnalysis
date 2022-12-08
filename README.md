# StockAnalysis

DEPLOYMENT INSTRUCTION
Download all US stocks the dataset from here.
Create a s3 bucket named “stock-analysis-emr-bucket”.
Make new directory inside the s3 bucket named “data-source”. 
Upload the Stocks folder from local to the directory “s3://stock-analysis-emr-bucket/data-source/”.
Create a new emr cluster with the following configurations:
Software Configuration : 
Application : Spark 2.4.8 on Hadoop 2.10.1 YARN and Zeppelin 0.10.0.
Hardware Configuration : 
Instance Type : m5.xlarge
Number of Instances : 3
Use the key pair security access for the EC2 Instances.
After creating the Cluster, SSH into the master node using the command 
ssh -i /path/key-pair-name.pem instance-user-name@instance-public-dns-name> 


Copy main.py file from local system to the master node
ssh -i /path/key-pair-name.pem instance-user-name@instance-public-dns-name> 


STEPS TO RUN THE APPLICATION
Connect to the master node to the emr cluster using ssh 
ssh -i /path/key-pair-name.pem instance-user-name@instance-public-dns-name> 


 To run the application, run the command 
spark-submit main.py


The Analysed data would get stored into the s3 bucket “s3://stock-analysis-emr-bucket/data-output”

TEST RESULTS	
On what day stock price was the highest?
all_data.orderBy(all_data["High"].desc()).head(1)[0][0]


Output: 2009-03-02

What is the average Closing price?
from pyspark.sql.functions import mean
all_data.select(mean("Close")).collect()[0][0]


Output: 30.3327


What is the maximum and minimum volume of stock traded?
all_data.select(max("Volume")).collect()[0][0]
all_data.select(min("Volume")).collect()[0][0]


Output: 10012
        9996



For how many days the closing value was less than 60 dollars?
all_data.filter(all_data['Close'] < 60).count()


Output: 3132

