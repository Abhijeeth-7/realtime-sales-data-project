import boto3
import base64
import random
import string
import datetime
import time
from uuid import uuid4
from botocore.exceptions import ClientError
import json

class MockDataGenerator:
	"""Generates mock data for sales transactions and inventory updates"""

	def __init__(self, base_timestamp=None):
		self.base_timestamp = base_timestamp

	def getUUID(self):
		return str(uuid4())

	def generate_sales_data(self):
		"""Generates a mock sales transaction data record"""
		transaction_id = self.getUUID()
		product_id = 'P' + ''.join(random.choices(string.digits, k=5))
		timestamp = self.get_current_timestamp()
		quantity = random.randint(0, 100)  # Quantity between 0 and 100
		unit_price = round(random.uniform(0, 50000), 2)  # Unit price between 0 and 50000 (2 decimal places)
		store_id = 'W' + ''.join(random.choices(string.digits, k=5))
		return {
				"transactionId": transaction_id,
				"productId": product_id,
				"timestamp": timestamp,
				"quantity": quantity,
				"unitPrice": unit_price,
				"storeId": store_id
		}

	def generate_inventory_data(self):
		"""Generates a mock inventory update data record"""
		product_id = 'P' + ''.join(random.choices(string.digits, k=3))
		timestamp = self.get_current_timestamp()
		quantity_change = random.randint(-100, 100)  # Quantity change between -100 and 100
		store_id = 'W' + ''.join(random.choices(string.digits, k=3))
		return {
				"id": self.getUUID(),
				"productId": product_id,
				"timestamp": timestamp,
				"quantityChange": quantity_change,
				"storeId": store_id
		}

	def get_current_timestamp(self):
		"""Gets the current timestamp in YYYY-MM-DD HH:MM:SS format"""
		return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

class KinesisClient:
	# Client for interacting with Amazon Kinesis Data Streams

	def __init__(self):
		self.session = boto3.Session()
		self.kinesis_client = self.session.client('kinesis')

	def put_record(self, data, stream_name, partition_key):
		#Sends a data record to the specified Kinesis stream.
		try:
			response = self.kinesis_client.put_record(
					StreamName=stream_name,
					Data=json.dumps(data).encode('utf-8'),
					PartitionKey=partition_key
			)
			return response
		except ClientError as error:
			print(f"Error putting record to {stream_name}: {error}")


if __name__ == '__main__':

	kinesis_client = KinesisClient()

	sales_stream = 'sales-transactions'
	sales_stream_partition_id = 'transactionId'
	
	inventory_stream = 'inventory-updates'
	inventory_stream_partition_id = 'id'

	data_generator = MockDataGenerator()

	while True:
		sales_transaction_data = data_generator.generate_sales_data()
		inventory_update_data = data_generator.generate_inventory_data()
		
		try:   
			response = kinesis_client.put_record(sales_transaction_data, sales_stream, sales_stream_partition_id)
			print(f"Successfully inserted the record.\nSales Stream\nShardId: {response['ShardId']}.\nSequence number: {response['SequenceNumber']}.\n Data: {sales_transaction_data}")
			
			response = kinesis_client.put_record(inventory_update_data, inventory_stream, inventory_stream_partition_id)
			print(f"Successfully inserted the record.\nInventory Stream\nShardId: {response['ShardId']}.\nSequence number: {response['SequenceNumber']}.\n Data: {sales_transaction_data}")
			
			time.sleep(random.randint(1,5))  # Simulate random delay between data points
		except KeyboardInterrupt:
			print("Data generation stopped by keyboard interrupt.")
			break

