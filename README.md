# realtime-sales-data-project

#### Architechture
![architechtural-diagram](https://github.com/user-attachments/assets/5a7b52f2-2290-4929-afaf-7135d53be641)

#### Detailed Explanation
- **Data Producer (Python Script):**
1. A Python script is developed to simulate or generate sales and inventory updates.
2. This script acts as a producer, streaming data to AWS Kinesis Data Streams.

- **Kinesis Data Streams**:
1. The sales and inventory updates are written directly to two separate Kinesis data streams.
2. One stream is dedicated to sales data, and the other to inventory updates.

-  **Firehose and Lambda Processing**:
1. Kinesis Firehose picks up data from the streams.
2. AWS Lambda functions are used to process the data from Firehose.
3. Lambda functions validate and transform the data.

- **Output to S3 and Redshift**:
1. Processed data is written to both S3 and Redshift tables.
2. Valid data is directed to Redshift for storage and analysis.
3. Invalid or failed data is sent back to a separate location in S3 for further inspection or reprocessing.

- **Separation of Streams**:
The architecture maintains two separate data streams to handle sales and inventory updates independently, ensuring efficient and organized data processing.
