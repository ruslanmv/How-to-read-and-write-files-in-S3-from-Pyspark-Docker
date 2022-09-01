# Read and Write files from S3 with  Pyspark Container

## Step 1 Geeting the AWS credentials

A simple way but without the envirooment you can use


```python
import os
# We assume that you have added your credential with $ aws configure
def get_aws_credentials():
    with open(os.path.expanduser("~/.aws/credentials")) as f:
        for line in f:
            #print(line.strip().split(' = '))
            try:
                key, val = line.strip().split(' = ')
                if key == 'aws_access_key_id':
                    aws_access_key_id = val
                elif key == 'aws_secret_access_key':
                    aws_secret_access_key = val
            except ValueError:
                pass
    return aws_access_key_id, aws_secret_access_key
```


```python
access_key, secret_key = get_aws_credentials()
```

For normal use we can export AWS CLI Profile to Environment Variables


```python
# Set environment variables
!export AWS_ACCESS_KEY_ID=$(aws configure get default.aws_access_key_id)
!export AWS_SECRET_ACCESS_KEY=$(aws configure get default.aws_secret_access_key)
```

and later load the enviroment variables in python.

## Step 2 Setup of Hadoop of the Container


```python
import sys
from random import random
from operator import add
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
```


```python
spark = SparkSession \
    .builder \
    .appName("Pyspark S3 reader") \
    .getOrCreate()
```

    WARNING: An illegal reflective access operation has occurred
    WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (file:/usr/local/spark-3.1.2-bin-hadoop3.2/jars/spark-unsafe_2.12-3.1.2.jar) to constructor java.nio.DirectByteBuffer(long,int)
    WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
    WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
    WARNING: All illegal access operations will be denied in a future release
    22/09/01 14:38:38 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
    Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
    Setting default log level to "WARN".
    To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
    22/09/01 14:38:39 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
    


```python
sc = spark.sparkContext

# remove this block if use core-site.xml and env variable
sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", secret_key)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", secret_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
sc._jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3.S3FileSystem")
```

## Step 3  Download you demo Dataset to the Container


```python
!wget https://github.com/ruslanmv/How-to-read-and-write-files-in-S3-from-Pyspark-Docker/raw/master/example/AMZN.csv
```

    --2022-09-01 14:38:46--  https://github.com/ruslanmv/How-to-read-and-write-files-in-S3-from-Pyspark-Docker/raw/master/example/AMZN.csv
    Resolving github.com (github.com)... 140.82.121.3
    Connecting to github.com (github.com)|140.82.121.3|:443... connected.
    HTTP request sent, awaiting response... 302 Found
    Location: https://raw.githubusercontent.com/ruslanmv/How-to-read-and-write-files-in-S3-from-Pyspark-Docker/master/example/AMZN.csv [following]
    --2022-09-01 14:38:46--  https://raw.githubusercontent.com/ruslanmv/How-to-read-and-write-files-in-S3-from-Pyspark-Docker/master/example/AMZN.csv
    Resolving raw.githubusercontent.com (raw.githubusercontent.com)... 185.199.109.133, 185.199.110.133, 185.199.111.133, ...
    Connecting to raw.githubusercontent.com (raw.githubusercontent.com)|185.199.109.133|:443... connected.
    HTTP request sent, awaiting response... 200 OK
    Length: 20032 (20K) [text/plain]
    Saving to: ‘AMZN.csv.4’
    
    AMZN.csv.4          100%[===================>]  19.56K  --.-KB/s    in 0.003s  
    
    2022-09-01 14:38:46 (6.42 MB/s) - ‘AMZN.csv.4’ saved [20032/20032]
    
    

## Step 3 Read the dataset present on local system


```python
df_AMZN=spark.read.csv('AMZN.csv',header=True,inferSchema=True)
df_AMZN.show(5)
```

    +----------+-----------+-----------+-----------+-----------+-----------+-------+
    |      Date|       Open|       High|        Low|      Close|  Adj Close| Volume|
    +----------+-----------+-----------+-----------+-----------+-----------+-------+
    |2020-02-10| 2085.01001|2135.600098|2084.959961|2133.909912|2133.909912|5056200|
    |2020-02-11|2150.899902|2185.949951|     2136.0|2150.800049|2150.800049|5746000|
    |2020-02-12|2163.199951|    2180.25|2155.290039|     2160.0|     2160.0|3334300|
    |2020-02-13| 2144.98999|2170.280029|     2142.0|2149.870117|2149.870117|3031800|
    |2020-02-14|2155.679932|2159.040039|2125.889893|2134.870117|2134.870117|2606200|
    +----------+-----------+-----------+-----------+-----------+-----------+-------+
    only showing top 5 rows
    
    

## Step 4 Creation of the S3 Bucket


```python
import boto3
```


```python
s3 = boto3.resource('s3')
# You should change the name the new bucket
my_new_bucket='stock-prices-pyspark'
s3.create_bucket(Bucket=my_new_bucket)
```




    s3.Bucket(name='stock-prices-pyspark')




```python
# You can list you latest Bucket Created 
!aws s3 ls --recursive | sort | tail -n 1 
```

    2022-08-31 21:59:41 stock-prices-pyspark
    

## Step 5. Write PySpark Dataframe to AWS S3 Bucket


```python
df_AMZN.write.format('csv').option('header','true').save('s3a://stock-prices-pyspark/csv/AMZN.csv',mode='overwrite')
```

    22/09/01 14:39:02 WARN MetricsConfig: Cannot locate configuration: tried hadoop-metrics2-s3a-file-system.properties,hadoop-metrics2.properties
    22/09/01 14:39:06 WARN AbstractS3ACommitterFactory: Using standard FileOutputCommitter to commit work. This is slow and potentially unsafe.
    22/09/01 14:39:08 WARN AbstractS3ACommitterFactory: Using standard FileOutputCommitter to commit work. This is slow and potentially unsafe.
                                                                                    

## Step 6. Read Data from AWS S3 into PySpark Dataframe


```python
s3_df=spark.read.csv("s3a://stock-prices-pyspark/csv/AMZN.csv",header=True,inferSchema=True)
s3_df.show(5)
```

                                                                                    

    +----------+-----------+-----------+-----------+-----------+-----------+-------+
    |      Date|       Open|       High|        Low|      Close|  Adj Close| Volume|
    +----------+-----------+-----------+-----------+-----------+-----------+-------+
    |2020-02-10| 2085.01001|2135.600098|2084.959961|2133.909912|2133.909912|5056200|
    |2020-02-11|2150.899902|2185.949951|     2136.0|2150.800049|2150.800049|5746000|
    |2020-02-12|2163.199951|    2180.25|2155.290039|     2160.0|     2160.0|3334300|
    |2020-02-13| 2144.98999|2170.280029|     2142.0|2149.870117|2149.870117|3031800|
    |2020-02-14|2155.679932|2159.040039|2125.889893|2134.870117|2134.870117|2606200|
    +----------+-----------+-----------+-----------+-----------+-----------+-------+
    only showing top 5 rows
    
    

## Step 7.  Read the files in the Bucket


```python
import boto3
import pandas as pd
bucket = "stock-prices-pyspark"
# We read the files in the Bucket
s3 = boto3.resource('s3')
my_bucket = s3.Bucket(bucket)
for file in my_bucket.objects.all():
    print(file.key)
```

    /csv/GOOG.csv
    csv/AMZN.csv/_SUCCESS
    csv/AMZN.csv/part-00000-2f15d0e6-376c-4e19-bbfb-5147235b02c7-c000.csv
    

## Step 8. Read Data from AWS S3 with boto3
If you dont need use Pyspark also you can read


```python
#We select one file of the bucket
bucket = "stock-prices-pyspark"
file_name = "csv/AMZN.csv/part-00000-2f15d0e6-376c-4e19-bbfb-5147235b02c7-c000.csv"
#s3 = boto3.client('s3') 
s3 =  boto3.client('s3', region_name='us-east-1')
# 's3' is a key word. create connection to S3 using default config and all buckets within S3
obj = s3.get_object(Bucket= bucket, Key= file_name) 
# get object and file (key) from bucket
df = pd.read_csv(obj['Body']) # 'Body' is a key word
```


```python
df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Date</th>
      <th>Open</th>
      <th>High</th>
      <th>Low</th>
      <th>Close</th>
      <th>Adj Close</th>
      <th>Volume</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2020-02-10</td>
      <td>2085.010010</td>
      <td>2135.600098</td>
      <td>2084.959961</td>
      <td>2133.909912</td>
      <td>2133.909912</td>
      <td>5056200</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2020-02-11</td>
      <td>2150.899902</td>
      <td>2185.949951</td>
      <td>2136.000000</td>
      <td>2150.800049</td>
      <td>2150.800049</td>
      <td>5746000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2020-02-12</td>
      <td>2163.199951</td>
      <td>2180.250000</td>
      <td>2155.290039</td>
      <td>2160.000000</td>
      <td>2160.000000</td>
      <td>3334300</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2020-02-13</td>
      <td>2144.989990</td>
      <td>2170.280029</td>
      <td>2142.000000</td>
      <td>2149.870117</td>
      <td>2149.870117</td>
      <td>3031800</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2020-02-14</td>
      <td>2155.679932</td>
      <td>2159.040039</td>
      <td>2125.889893</td>
      <td>2134.870117</td>
      <td>2134.870117</td>
      <td>2606200</td>
    </tr>
  </tbody>
</table>
</div>



## Step 9 - Downloading Multiple Files locally with wget
If you want to download multiple files at once, use the -i option followed by the path to a local or external file containing a list of the URLs to be downloaded. Each URL needs to be on a separate line.

We create the file list to download


```python
with open("datasets.txt","a") as file:
    file.write("https://github.com/ruslanmv/How-to-read-and-write-files-in-S3-from-Pyspark-Docker/raw/master/example/AMZN.csv\n")
    file.write("https://github.com/ruslanmv/How-to-read-and-write-files-in-S3-from-Pyspark-Docker/raw/master/example/GOOG.csv\n")
    file.write("https://github.com/ruslanmv/How-to-read-and-write-files-in-S3-from-Pyspark-Docker/raw/master/example/TSLA.csv\n")
```


```python
# we download all the files
!wget -q -i datasets.txt
```


```python
import boto3
bucket = "stock-prices-pyspark"
s3 = boto3.resource('s3')
s3.meta.client.upload_file('GOOG.csv', bucket, 'csv/'+'GOOG.csv')
```


```python
!aws s3 ls
```

    2022-01-24 14:42:55 datalake-temporal-proyect-240122
    2022-07-25 21:31:05 mysound-s3-ruslanmv
    2022-08-03 19:50:23 sagemaker-studio-342527032693-bas5sukiu4c
    2022-08-31 21:59:41 stock-prices-pyspark
    2022-08-31 21:22:06 stock-prices-spark
    


```python
spark.stop()
```
