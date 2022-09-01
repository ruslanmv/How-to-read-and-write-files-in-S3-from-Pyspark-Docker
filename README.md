# How to read and write files from S3 buckets with Pyspark in a Docker Container

Hello everyone, today we are going  create a custom Docker Container with **JupyterLab**  with **Pyspark**  that will read files from **AWS S3**

## Introduction

If you need to read your files in **S3 Bucket**  from **any computer** you need only do few steps:

1. Install  [Docker]([https://www.docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop)).

2. Run this command:

```
docker run -p 8888:8888 ruslanmv/pyspark-aws:3.1.2
```

3. Open web browser and paste link of your previous step

4. Open a new terminal 

   ![image-20220831224711838](assets/images/posts/README/image-20220831224711838.png)

5. Run the command:

   ```
   aws configure
   ```

   The following example shows sample values.

   ```
   AWS Access Key ID [None]: AKIAIOSFODNN7EXAMPLE
   AWS Secret Access Key [None]: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
   Default region name [None]: us-west-2
   Default output format [None]: json
   ```

6. Open a new notebook

   ![image-20220831224818375](assets/images/posts/README/image-20220831224818375.png)

7.  If you want read the files in you bucket, replace BUCKET_NAME 

   ```python
   import boto3
   s3 = boto3.resource('s3')
   my_bucket = s3.Bucket('BUCKET_NAME')
   for file in my_bucket.objects.all():
       print(file.key)
   ```

   ![image-20220831232436495](assets/images/posts/README/image-20220831232436495.png)

   

Good !,  you have seen how simple is read the files inside a S3 bucket within  boto3. 

In the following sections  I will explain in more details how to create this container and how to read an write by using this container.



# Getting started with pyspark-aws container



## Step 1 Installation of  Docker

If you are in Linux, using Ubuntu, you can create an script file called **install_docker.sh** and paste the following code

<script src="https://gist.github.com/ruslanmv/511d96c2d9cc2dd3b68a67490bcf9aad.js"></script>



This script is compatible with any EC2 instance with **Ubuntu 22.04 LSTM,** then just type `sh install_docker.sh` in the terminal.

![image-20220831205814471](assets/images/posts/README/image-20220831205814471.png)



If you are using **Windows 10/11**, for example in your Laptop, You can install the docker Desktop

[https://www.docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop)



## Step 2 Creation of the  Container

If you want create your own Docker Container you can create  [Dockerfile ](https://gist.github.com/ruslanmv/9518aa1113c48a9002266f7bd3b012a0#file-dockerfile)  and [requirements.txt ](https://gist.github.com/ruslanmv/9518aa1113c48a9002266f7bd3b012a0#file-requirements-txt) with the following:

<script src="https://gist.github.com/ruslanmv/9518aa1113c48a9002266f7bd3b012a0.js"></script>

then in the terminal type

```
docker build --rm -t ruslanmv/pyspark-aws .
```

and then you will have:

![image-20220831213807833](assets/images/posts/README/image-20220831213807833.png)



### Step 3. Running the container

Setting up a Docker container on your local machine is pretty simple. Simply we run the following command in the terminal:

```
docker run  --name pyspark-aws  -it -p 8888:8888 -d ruslanmv/pyspark-aws
```

