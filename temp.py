import boto3
import json
import base64

def insert_into_redshift(data):

    redshift_client = boto3.client('redshift-data', region_name='us-east-1')

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
        print('executionResult\n', response)
         
        from time import sleep
        while True:
            response2 = redshift_client.describe_statement(
                Id=response['Id'],
            )
            print('Status', response2['Status']) 
            if response2['Status'] in ["FINISHED", "FAILED"]: 
                if "Error" in response2: 
                    print("error -> \n\t", response2["Error"])
                break
            sleep(3)
        
        if response2["HasResultSet"]:
            response3 = redshift_client.get_statement_result(
                Id=response['Id'],
            )
            print('\nresult\n', *response3['Records']) 
        
    except Exception as e:
        print(f"Error inserting data: {e}")

# Example data
data = [
	{'name': 'transactionId', 'value': 'T1001'},
	{'name': 'productId', 'value': 'P501'},
	{'name': 'timestamp', 'value': '2023-11-24 00:01:00'},
	{'name': 'quantity', 'value': '2'},
	{'name': 'unitPrice', 'value': '299.99'},
	{'name': 'storeId', 'value': 'W001'}
]

insert_into_redshift(data)