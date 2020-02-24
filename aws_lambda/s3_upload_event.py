import json
import urllib.parse
import boto3
from botocore.exceptions import ClientError
import os


print('Loading function')

s3 = boto3.client('s3')
job_flow_id = os.environ['emr_cluster_id']

def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'],
                                    encoding='utf-8')
                                    
    path = os.path.dirname(key)
    try:
        # Define a job flow step.
        job_flow_step_01 = {
            'Name': 'WordCount',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 's3://s3801950-wordcount/jar/wordcount-1.1.jar',
                'Args': [
                    's3://' + bucket + '/' + key,
                    's3://' + bucket + '/' + path + '/output/',
                ]
            }
        }
        
        emr_client = boto3.client('emr', 'us-east-1')
        try:
            file = emr_client.add_job_flow_steps(JobFlowId=job_flow_id,
                                                 Steps=[job_flow_step_01])
        except ClientError as e:
            print("error connecting with emr_client")
            print(e.response['Error']['Message'])
            exit(1)

        # Output the IDs of the added steps
        print('Step IDs:')
        for stepId in file['StepIds']:
            print(stepId)

    except Exception as e:
        print("Some other error:")
        raise e