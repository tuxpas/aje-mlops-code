import boto3
import datetime
import os

def lambda_handler(event, context):
    client = boto3.client('sagemaker')

    code_country = (
        event.get("code_country") or 
        os.environ.get("code_country") or 
        "EC"
    ).upper()

    execution_name = f"ps-{code_country}-{datetime.datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"

    try:
        response = client.start_pipeline_execution(
            PipelineName='Pipeline-PedidoSugerido-OneRun',
            PipelineExecutionDisplayName=execution_name,
            PipelineParameters=[
                {"Name": "country", "Value": code_country}
            ]
        )

        return {
            "statusCode": 200,
            "executionArn": response["PipelineExecutionArn"],
            "executionName": execution_name,
            "country": code_country
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "error": str(e),
            "country": code_country
        }