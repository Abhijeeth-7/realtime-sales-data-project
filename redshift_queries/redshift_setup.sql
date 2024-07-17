-- Create an external schema
CREATE EXTERNAL SCHEMA BlackFridaySale_external_schema
FROM DATA CATALOG
DATABASE 'dev'
IAM_ROLE 'arn:aws:iam::894063495656:role/service-role/AmazonRedshift-CommandsAccessRole-20240630T163532'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- Create external tables for preloaded data
CREATE EXTERNAL TABLE BlackFridaySale_external_schema.products (
    productId VARCHAR(50),
    name VARCHAR(255),
    category VARCHAR(255),
    price DECIMAL(10, 2),
    supplierId VARCHAR(50)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://blackfriday-sale-data-processed/preloaded-data/products.csv';

CREATE EXTERNAL TABLE BlackFridaySale_external_schema.stores (
    storeId VARCHAR(50),
    location VARCHAR(255),
    size INT,
    manager VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://blackfriday-sale-data-processed/preloaded-data/stores.csv';

-- Create tables for fact tables
CREATE TABLE IF NOT EXISTS salesTransactions (
    transactionId VARCHAR(50) NOT NULL,
    productId VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    quantity INT NOT NULL,
    unitPrice DECIMAL(10, 2) NOT NULL,
    storeId VARCHAR(50) NOT NULL,
    PRIMARY KEY (transactionId)
);

CREATE TABLE IF NOT EXISTS inventoryUpdates (
    productId VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    quantityChange INT NOT NULL,
    storeId VARCHAR(50) NOT NULL,
);
