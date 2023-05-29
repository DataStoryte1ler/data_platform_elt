import boto3
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

def process_data():
    # Creating an S3 client
    s3_client = boto3.client('s3')

    # Specifying the S3 bucket and folder path
    s3_bucket = 'data-platform-raw-data-storage'
    s3_folder_path = 'csv_data'
    access_key = 'some_key'
    secret_access_key = 'some_key'

    conf = SparkConf()
    conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
    conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    conf.set('spark.hadoop.fs.s3a.access.key', access_key)
    conf.set('spark.hadoop.fs.s3a.secret.key', secret_access_key)
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # Listing all objects in the S3 folder
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_folder_path)

    # Creating dictionaries to store the DataFrames
    dataframes = {} # It is where our raw data is stored
    cleaned_dataframes = {} # It is where our cleaned data is stored

    # Reading each CSV file and create a DataFrame
    for obj in response['Contents']:
        file_key = obj['Key']
        file_name = file_key.split('/')[-1]

        if file_name.endswith('.csv'):
            # Read CSV directly from S3 into DataFrame
            dataframe = spark.read.csv(f"s3a://{s3_bucket}/{file_key}", header=True, inferSchema=True)
            dataframes[file_name] = dataframe

    # Performing quality checks on dataframes
    for filename, dataframe in dataframes.items():
        print(f"DataFrame for {filename}:")
        dataframe.show(5)  # Display the first 5 rows of each DataFrame

        # Checking for duplicate rows in each dataframe
        duplicate_rows = dataframe.groupBy(list(dataframe.columns)).count().filter(col("count") > 1)
        if duplicate_rows.count() > 0:
            print(f"WARNING: DataFrame {filename} has {duplicate_rows.count()} duplicate rows.")
            # Removing duplicate rows from the dataframe
            dataframe = dataframe.dropDuplicates()
        else:
            print("SUCCESS: No duplicates found.")
        
        # Checking for null values in each dataframe
        null_counts = dataframe.select([col(column).isNull().cast("int").alias(column) for column in dataframe.columns])
        # Replacing null values with a default value
        for column in dataframe.columns:
            default_value = 0 
            dataframe = dataframe.na.fill({column: default_value})

        # Storing the cleaned dataframes in the new dictionary
        cleaned_dataframes[filename] = dataframe


    # Creating separate dataframe for each file
    customers_df = cleaned_dataframes['customers.csv']
    geolocation_df = cleaned_dataframes['geolocation.csv']
    order_payments_df = cleaned_dataframes['order_payments.csv']
    order_reviews_df = cleaned_dataframes['order_reviews.csv']
    orders_df = cleaned_dataframes['orders.csv']
    products_df = cleaned_dataframes['products.csv']
    sellers_df = cleaned_dataframes['sellers.csv']
    order_items_df = cleaned_dataframes['order_items.csv']


    # Performing transformations for Customers dataframe and preparing data for further loading into DWH
    customer_dim = customers_df.select(
        'customer_id',
        'customer_zip_code_prefix',
        'customer_city', 
        'customer_state'
        )
    customer_dim.show(10)
    customer_dim.printSchema()


    # Performing transformations for Products dataframe and preparing data for further loading into DWH
    product_dim = products_df.select(
        'product_id',
        'product_category_name',
        'product_photos_qty',
        'product_weight_g',
        'product_length_cm',
        'product_height_cm',
        'product_width_cm'
    )


    # Performing transformations for Order_Items dataframe and preparing data for further loading into DWH
    order_items_dim = order_items_df.select(
        'order_item_id',
        'order_id',
        'product_id',
        'seller_id',
        'shipping_limit_date',
        'price',
        'freight_value'
    )
    order_items_dim = order_items_dim.dropDuplicates(["order_id"])


    # Performing transformations for Order_Reviews dataframe and preparing data for further loading into DWH
    order_reviews_dim = order_reviews_df
    order_reviews_dim = order_reviews_dim.withColumn('review_score', col('review_score').cast('float'))
    order_reviews_dim = order_reviews_dim.withColumn("review_creation_date", to_timestamp(col("review_creation_date"), "yyyy-MM-dd HH:mm:ss"))
    order_reviews_dim = order_reviews_dim.withColumn("review_answer_timestamp", to_timestamp(col("review_answer_timestamp"), "yyyy-MM-dd HH:mm:ss"))
    order_reviews_dim = order_reviews_dim.na.drop(subset=['review_creation_date', 'review_answer_timestamp'])
    order_reviews_dim = order_reviews_dim.dropDuplicates(['review_id'])


    # Performing transformations for Geolocation dataframe and preparing data for further loading into DWH
    geolocation_dim = geolocation_df
    geolocation_dim = geolocation_dim.withColumnRenamed('geolocation_zip_code_prefix', 'zip_code_prefix')
    geolocation_dim = geolocation_dim.withColumnRenamed('geolocation_lat', 'latitude')
    geolocation_dim = geolocation_dim.withColumnRenamed('geolocation_lng', 'longitude')
    geolocation_dim = geolocation_dim.withColumnRenamed('geolocation_city', 'city')
    geolocation_dim = geolocation_dim.withColumnRenamed('geolocation_state', 'state')
    geolocation_dim = geolocation_dim.dropDuplicates(["zip_code_prefix"])


    # Performing transformations for Sellers dataframe and preparing data for further loading into DWH
    seller_dim = sellers_df


    # Performing transformations for Orders dataframe and preparing data for further loading into DWH
    order_fact = orders_df.join(
        order_payments_df,
        orders_df['order_id'] == order_payments_df['order_id'],
        how='inner'
        ).join(
            customers_df,
            orders_df['customer_id'] == customers_df['customer_id'],
            how='inner'
            ).join(
                order_items_df,
                orders_df['order_id'] == order_items_df['order_id'],
                how='inner'
                ).join(
                    sellers_df,
                    order_items_df['seller_id'] == sellers_df['seller_id'],
                    how='inner'
                    ).join(
                        order_reviews_df,
                        orders_df['order_id'] == order_reviews_df['order_id'],
                        how='inner'
                        ).join(
                            geolocation_df,
                            geolocation_df['geolocation_zip_code_prefix'] == customers_df['customer_zip_code_prefix'],
                            how='inner'
                            ).select(
                                orders_df.order_id,
                                customers_df.customer_id,
                                sellers_df.seller_id,
                                geolocation_df.geolocation_zip_code_prefix,
                                order_reviews_df.review_id,
                                orders_df.order_status,
                                orders_df.order_purchase_timestamp,
                                orders_df.order_approved_at,
                                orders_df.order_delivered_carrier_date,
                                orders_df.order_delivered_customer_date,
                                orders_df.order_estimated_delivery_date,
                                order_payments_df.payment_sequential,
                                order_payments_df.payment_type,
                                order_payments_df.payment_installments,
                                order_payments_df.payment_value
                                )

    order_fact = order_fact.withColumnRenamed('geolocation_zip_code_prefix', 'zip_code_prefix')
    order_fact = order_fact.withColumn("order_delivered_carrier_date", to_timestamp(col("order_delivered_carrier_date"), "yyyy-MM-dd HH:mm:ss"))
    order_fact = order_fact.filter(col('order_delivered_carrier_date') != '1970-01-01 03:00:00')
    order_fact = order_fact.withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date"), "yyyy-MM-dd HH:mm:ss"))
    order_fact = order_fact.filter(col('order_delivered_customer_date') != '1970-01-01 03:00:00')
    order_fact = order_fact.withColumn("order_approved_at", to_timestamp(col("order_approved_at"), "yyyy-MM-dd HH:mm:ss"))
    order_fact = order_fact.filter(col('order_approved_at') != '1970-01-01 03:00:00')
    order_fact = order_fact.withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"), "yyyy-MM-dd HH:mm:ss"))
    order_fact = order_fact.filter(col('order_purchase_timestamp') != '1970-01-01 03:00:00')
    order_fact = order_fact.withColumn("order_estimated_delivery_date", to_timestamp(col("order_estimated_delivery_date"), "yyyy-MM-dd HH:mm:ss"))
    order_fact = order_fact.filter(col('order_estimated_delivery_date') != '1970-01-01 03:00:00')
    order_fact = order_fact.dropDuplicates(['order_id'])
    order_fact = order_fact.withColumn('review_id', order_reviews_dim['review_id'])


    # Defining function for uploading each transformed dataframe into MySQL DWH
    def write_to_mysql(dbtable, df):
        mysql_properties = {
            "driver": "com.mysql.cj.jdbc.Driver",
            "url": "jdbc:mysql://localhost:3306/warehouse",
            "user": "root",
            "password": "root",
            "dbtable": dbtable
        }

        df.write \
            .format("jdbc") \
            .mode("append") \
            .options(**mysql_properties) \
            .save()

    write_to_mysql('Customers', customer_dim)
    write_to_mysql('Products', product_dim)
    write_to_mysql('Order_Items', order_items_dim)
    write_to_mysql('Order_Reviews', order_reviews_dim)
    write_to_mysql('Geolocation', geolocation_dim)
    write_to_mysql('Sellers', seller_dim)
    write_to_mysql('Orders', order_fact)


    # Stoping the SparkSession
    spark.stop()
