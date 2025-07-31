from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from datetime import datetime
import os
import boto3
import json
from botocore.exceptions import NoCredentialsError, ClientError

default_args = {
    'owner': 'karan',
    'start_date': datetime(2025, 7, 29),
    'retries': 1,
}

CLUSTER_ID = "j-2959IBUNK7O0U"  # Update with real EMR cluster ID
BUCKET_URI = "s3://dmartscripts-buc/scripts/"
AWS_REGION = 'ap-south-1'
LAMBDA_FUNCTION_NAME = 'load_to_rds'
S3_PROCESSED_PATH = 's3://dmartsales-buc/processed_sales_dataset/'


# ---------------------- Task Functions ---------------------- #

def upload_folder_to_s3():
    local_folder = '/home/karanvm/Sales_Dataset'
    bucket_name = 'dmartsales-buc'
    s3_folder = 'sales_dataset/'

    s3 = boto3.client('s3')
    for root, dirs, files in os.walk(local_folder):
        for file in files:
            local_path = os.path.join(root, file)
            s3_path = os.path.join(s3_folder, file)
            try:
                s3.upload_file(local_path, bucket_name, s3_path)
                print(f"Uploaded: {local_path} to s3://{bucket_name}/{s3_path}")
            except FileNotFoundError:
                print(f"File not found: {local_path}")
            except NoCredentialsError:
                print("AWS credentials not available.")
            except Exception as e:
                print(f"Failed to upload {local_path}: {e}")

def trigger_glue_job():
    glue_client = boto3.client('glue', region_name='ap-south-1')
    try:
        response = glue_client.start_job_run(
            JobName='sales-etl-job',
            Arguments={
                '--source_path': 's3://dmartsales-buc/sales_dataset/',
                '--target_path': 's3://dmartsales-buc/processed_sales_dataset/',
            }
        )
        job_run_id = response['JobRunId']
        print(f"Glue job started with Run ID: {job_run_id}")
        return job_run_id
    except ClientError as e:
        print(f"Error triggering Glue job: {e}")
        raise


def invoke_lambda_function():
    client = boto3.client('lambda', region_name=AWS_REGION)
    payload = {"s3_path": S3_PROCESSED_PATH}
    response = client.invoke(
        FunctionName=LAMBDA_FUNCTION_NAME,
        InvocationType='Event',
        Payload=json.dumps(payload)
    )
    print(f"Lambda invoked: {response['StatusCode']}")
    




def features_local_to_s3():
    s3_client = boto3.client('s3')
    local_folder = '/home/karanvm/Features_Dataset'
    s3_bucket = 'dmartfeatures-buc'
    s3_folder = 'features_dataset'

    for root, dirs, files in os.walk(local_folder):
        for file in files:
            local_path = os.path.join(root, file)
            s3_path = os.path.join(s3_folder, file)
            try:
                s3_client.upload_file(local_path, s3_bucket, s3_path)
                print(f"Uploaded {local_path} to s3://{s3_bucket}/{s3_path}")
            except Exception as e:
                print(f"Failed to upload {local_path} to S3: {e}")

# ---------------------- EMR Steps ---------------------- #
step1 = {
    'Name': 'Run shell script s3_to_hdfs',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            'bash', '-c',
            f'aws s3 cp {BUCKET_URI}s3_to_hdfs.sh . && chmod +x s3_to_hdfs.sh && bash s3_to_hdfs.sh'
        ]
    }
}


step2 = {
    'Name': 'Run python script hdfs_to_hive',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            'bash', '-c',
            f'aws s3 cp {BUCKET_URI}hdfs_to_hive.py . && spark-submit hdfs_to_hive.py'
        ]
    }
}


step3 = {
    'Name': 'Run shell script sqoop',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            'bash', '-c',
            f'aws s3 cp {BUCKET_URI}sqoop_import.sh . && chmod +x sqoop_import.sh && bash sqoop_import.sh'
        ]
    }
}


step4 = {
    'Name': 'Run python script final_result',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            'bash', '-c',
            f'aws s3 cp {BUCKET_URI}final_result.py . && spark-submit final_result.py'
        ]
    }
}



# ---------------------- DAG Definition ---------------------- #
@dag(
    dag_id='dmart_data_analytics_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False,
    description='retailstream comprehensive data integration and anaytics pipeline'
)
def sales_pipeline():

    upload_task1 = PythonOperator(
        task_id='upload_sales_files',
        python_callable=upload_folder_to_s3
    )

    run_glue_etl = PythonOperator(
        task_id='run_glue_job',
        python_callable=trigger_glue_job
    )


    wait_for_glue = GlueJobSensor(
    	task_id='wait_for_glue_job',
    	job_name='sales-etl-job',
    	run_id="{{ task_instance.xcom_pull(task_ids='run_glue_job') }}",
    	aws_conn_id='aws_default'
    )


    trigger_lambda = PythonOperator(
        task_id='invoke_lambda',
        python_callable=invoke_lambda_function
    )

    upload_task2 = PythonOperator(
        task_id='upload_features_files',
        python_callable=features_local_to_s3
    )

    add_step1 = EmrAddStepsOperator(
        task_id='add_step1',
        job_flow_id=CLUSTER_ID,
        steps=[step1],
        aws_conn_id='aws_default'
    )

    wait_step1 = EmrStepSensor(
        task_id='wait_step1',
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='add_step1', key='return_value')[0] }}",
        aws_conn_id='aws_default'
    )

    add_step2 = EmrAddStepsOperator(
        task_id='add_step2',
        job_flow_id=CLUSTER_ID,
        steps=[step2],
        aws_conn_id='aws_default'
    )

    wait_step2 = EmrStepSensor(
        task_id='wait_step2',
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='add_step2', key='return_value')[0] }}",
        aws_conn_id='aws_default'
    )

    add_step3 = EmrAddStepsOperator(
        task_id='add_step3',
        job_flow_id=CLUSTER_ID,
        steps=[step3],
        aws_conn_id='aws_default'
    )

    wait_step3 = EmrStepSensor(
        task_id='wait_step3',
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='add_step3', key='return_value')[0] }}",
        aws_conn_id='aws_default'
    )

    add_step4 = EmrAddStepsOperator(
        task_id='add_step4',
        job_flow_id=CLUSTER_ID,
        steps=[step4],
        aws_conn_id='aws_default'
    )

    wait_step4 = EmrStepSensor(
        task_id='wait_step4',
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='add_step4', key='return_value')[0] }}",
        aws_conn_id='aws_default'
    )

    # Task chaining
    upload_task1 >> run_glue_etl >> wait_for_glue >> trigger_lambda >> upload_task2 >> add_step1 >> wait_step1
    wait_step1 >> add_step2 >> wait_step2
    wait_step2 >> add_step3 >> wait_step3
    wait_step3 >> add_step4 >> wait_step4


dag_instance = sales_pipeline()
