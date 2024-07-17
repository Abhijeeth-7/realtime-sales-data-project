import json
import boto3
from uuid import uuid4
from datetime import datetime
from BlackFridaySales.temp import insert_into_redshift


def validate_sales_data(data):
    errors = []

    # Check for all required columns
    required_columns = ["id", "productId", "timestamp", "quantityChange", "storeId"]
    if not all(col in data for col in required_columns):
        errors.append(
            f"Missing required columns: {', '.join(set(required_columns) - set(data.keys()))}"
        )
        return False

    # Check for invalid values
    if not isinstance(data["id"], str) or len(data["id"]) == 0:
        errors.append(f"Invalid id: {data['id']}")
    if not isinstance(data["productId"], str) or len(data["productId"]) == 0:
        errors.append(f"Invalid productId: {data['productId']}")
    try:
        datetime.fromisoformat(data["timestamp"])  # Check timestamp format (ISO 8601)
    except ValueError:
        errors.append(f"Invalid timestamp format: {data['timestamp']}")
    if not isinstance(data["quantityChange"], int) or data["quantityChange"] < 0:
        errors.append(f"Invalid quantityChange: {data['quantityChange']}")
    if not isinstance(data["storeId"], float) or data["storeId"] <= 0:
        errors.append(f"Invalid storeId: {data['storeId']}")

    return {"result": not errors, "validationMessages": errors}


def insert_into_redshift(data):

    insert_query = """
		INSERT INTO public.inventoryUpdates
		(productid, quantityChange, timestamp, storeid)
		VALUES (:productId, :quantityChange, :timestamp, :storeId);
	"""

    try:
        response = redshift_client.execute_statement(
            WorkgroupName="data-engineering-workgroup",
            Database="dev",
            Sql=insert_query,
            Parameters=data,
        )
        print(f'inserted data, event id {response["Id"]}')

    except Exception as e:
        print(f"Error while inserting data: {e}\n Data {data.values()}")


def lambda_handler(event, context):
    redshift_client = boto3.client("redshift-data")

    # process kenisis record
    for record in event["records"]:

        payload = base64.b64decode(record["data"])
        sales_data = json.loads(payload)

        try:
            validation_result = validate_sales_data(data)
            if validation_result["result"]:

                inv_update_to_insert = [
                    {"name": key, "value": value} for key, value in sales_data.items()
                ]
                insert_into_redshift(inv_update_to_insert)

                transformed_data_str = json.dumps(transformed_data) + "\n"
                transformed_data_encoded = base64.b64encode(
                    transformed_data_str.encode("utf-8")
                ).decode("utf-8")

                result.append(
                    {
                        "recordId": record["recordId"],
                        "result": "Ok",
                        "data": transformed_data_encoded,
                    }
                )
            else:
                raise Exception(
                    f'Data quality check failed\n errorMessage: {validation_result["validationMessages"]}'
                )
        except Exception as e:
            print(f"Error while inserting record to redshift.\n Error: {e}")

    return {"records": result}
