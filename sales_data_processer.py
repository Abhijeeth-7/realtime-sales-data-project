import json
import boto3
from uuid import uuid4
from datetime import datetime
from BlackFridaySales.temp import insert_into_redshift

def validate_sales_data(data):
	errors = []

	# Check for all required columns
	required_columns = ["transactionId", "productId", "timestamp", "quantity", "unitPrice"]
	if not all(col in data for col in required_columns):
		errors.append(f"Missing required columns: {', '.join(set(required_columns) - set(data.keys()))}")
		return False

	# Check for invalid values
	if not isinstance(data["transactionId"], str) or len(data["transactionId"]) == 0:
		errors.append(f"Invalid transactionId: {data['transactionId']}")
	if not isinstance(data["productId"], str) or len(data["productId"]) == 0:
		errors.append(f"Invalid productId: {data['productId']}")
	try:
		datetime.fromisoformat(data["timestamp"])  # Check timestamp format (ISO 8601)
	except ValueError:
		errors.append(f"Invalid timestamp format: {data['timestamp']}")  
	if not isinstance(data["quantity"], int) or data["quantity"] < 0:
		errors.append(f"Invalid quantity: {data['quantity']}")
	if not isinstance(data["unitPrice"], float) or data["unitPrice"] <= 0:
		errors.append(f"Invalid unitPrice: {data['unitPrice']}")

	return {'result': not errors, 'validationMessages': errors}
	
def insert_into_redshift(data):

	insert_query = """
		INSERT INTO public.salestransactions
		(transactionid, productid, timestamp, quantity, unitprice, storeid)
		VALUES (:transactionId, :productId, :timestamp, :quantity, :unitPrice, :storeId);
	"""
	
	try:
		response = redshift_client.execute_statement(
			WorkgroupName = "data-engineering-workgroup",
			Database="dev",
			Sql = insert_query,
			Parameters = data
		)
		print(f'inserted data, event id {response["Id"]}') 
		
	except Exception as e:
		print(f"Error while inserting data: {e}\n Data {data.values()}")

def lambda_handler(event, context):
	redshift_client = boto3.client('redshift-data')

	#process kenisis record
	for record in event['records']:

		payload = base64.b64decode(record['data'])
		sales_data = json.loads(payload)

		try:
			validation_result =  validate_sales_data(data)
			if validation_result["result"]:
				
				sales_data_to_insert = [{'name': key, 'value': value} for key, value in sales_data.items()]
				insert_into_redshift(sales_data_to_insert)
		
				transformed_data_str = json.dumps(transformed_data) + '\n'
				transformed_data_encoded = base64.b64encode(transformed_data_str.encode('utf-8')).decode('utf-8')

				result.append({
					'recordId': record['recordId'],
					'result': 'Ok',
					'data': transformed_data_encoded
				})
			else:
				raise Exception(f'Data quality check failed\n errorMessage: {validation_result["validationMessages"]}')
		except Exception as e:
			print(f"Error while inserting record to redshift.\n Error: {e}")

	return {
		'records': result
	}
