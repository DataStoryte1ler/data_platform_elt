import mysql.connector
from mysql.connector import Error

def create_tables():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='warehouse',
            user='root',
            password='root'
        )
        
        if connection.is_connected():
            print('Connected to MySQL database')

        create_table_query = '''
            CREATE TABLE Customers (
                customer_id VARCHAR(255) PRIMARY KEY,
                customer_zip_code_prefix INT,
                customer_city VARCHAR(100),
                customer_state CHAR(2)
            );

            CREATE TABLE Products (
                product_id VARCHAR(255) PRIMARY KEY,
                product_category_name VARCHAR(255),
                product_photos_qty SMALLINT,
                product_weight_g INT,
                product_length_cm INT,
                product_height_cm INT,
                product_width_cm INT
            );

            CREATE TABLE Order_Reviews (
                review_id VARCHAR(255) PRIMARY KEY,
                order_id VARCHAR(255),
                review_score FLOAT,
                review_comment_title VARCHAR(255),
                review_comment_message TEXT,
                review_creation_date TIMESTAMP,
                review_answer_timestamp TIMESTAMP
            );

            CREATE TABLE Geolocation (
                zip_code_prefix INT PRIMARY KEY,
                latitude DOUBLE,
                longitude DOUBLE,
                city VARCHAR(100),
                state CHAR(2)
            );

            CREATE TABLE Sellers (
                seller_id VARCHAR(255) PRIMARY KEY,
                seller_zip_code_prefix INT,
                seller_city VARCHAR(100),
                seller_state CHAR(2)
            );

            CREATE TABLE Order_Items (
                order_item_id SMALLINT,
                order_id VARCHAR(255),
                product_id VARCHAR(255),
                seller_id VARCHAR(255),
                shipping_limit_date TIMESTAMP,
                price DECIMAL(10,2),
                freight_value DECIMAL(10,2),
                PRIMARY KEY (order_id, order_item_id),
                FOREIGN KEY (product_id) REFERENCES Products(product_id),
                FOREIGN KEY (seller_id) REFERENCES Sellers(seller_id)
            );

            CREATE TABLE Orders (
                order_id VARCHAR(255) PRIMARY KEY,
                customer_id VARCHAR(255),
                seller_id VARCHAR(255),
                zip_code_prefix INT,
                review_id VARCHAR(255),
                order_status VARCHAR(100),
                order_purchase_timestamp TIMESTAMP,
                order_approved_at TIMESTAMP,
                order_delivered_carrier_date TIMESTAMP,
                order_delivered_customer_date TIMESTAMP,
                order_estimated_delivery_date TIMESTAMP,
                payment_sequential SMALLINT,
                payment_type VARCHAR(100),
                payment_installments SMALLINT,
                payment_value DECIMAL(10,2),
                FOREIGN KEY (customer_id) REFERENCES Customers (customer_id),
                FOREIGN KEY (seller_id) REFERENCES Sellers (seller_id),
                FOREIGN KEY (zip_code_prefix) REFERENCES Geolocation (zip_code_prefix),
                FOREIGN KEY (review_id) REFERENCES Order_Reviews (review_id)
            );
            
        '''

        cursor = connection.cursor()
        cursor.execute(create_table_query)
        print('Tables created successfully')

        cursor.close()
        connection.close()

    except Error as e:
        print(f'Error connecting to MySQL database: {e}')
