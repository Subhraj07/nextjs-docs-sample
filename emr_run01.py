import time
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import sys

FILE_PATH = "s3://svc-java-mwes-realtime-emr-prod/prod/deployment/latest/event_streaming/mwes_event_streaming.py"
CHECKPOINT_PATH = "s3://svc-java-mwes-realtime-emr-prod/real_time_stream/aerospike_write/mw_global_level_aggregation/campaign15/_checkpoint/"
OUTPUT_PATH = "s3://svc-java-mwes-realtime-emr-prod/real_time_stream/aerospike_write/mw_global_level_aggregation/output_dir/"
AEROSPIKE_HOST = "bidder-intelligence-aerospike.prod.platform.ext.mobilityware.com"
KAFKA_HOST = "kafka.prod.platform.ext.mobilityware.com:9095"
CLUSTER_ID = "j-21G0OWQNYIYL1"
INTERVAL = 60

def create_step_config(original_step):
    """
    Creates a new step configuration specifically for Spark steps using command-runner.jar
    """
    return {
        'Name': f"{original_step['Name']}",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit',
                    '--deploy-mode', 'cluster',
                    '--packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1',
                    FILE_PATH,
                    CHECKPOINT_PATH,
                    OUTPUT_PATH,
                    AEROSPIKE_HOST,
                    KAFKA_HOST,
                    'tracker_events_dump_v6',
                    'insertRecordMapWithMap',
                    'createOrUpdateRecordMAB',
                    'createOrUpdateContextMABV1',
                    'createOrUpdateCreativeMAB'
            ]
        }
    }

def get_latest_step(emr_client, cluster_id):
    """
    Get the latest step from the EMR cluster
    """
    try:
        response = emr_client.list_steps(
            ClusterId=cluster_id,
            StepStates=['PENDING', 'RUNNING', 'FAILED', 'CANCELLED']  # Only get active or recently ended steps
        )
        
        if not response['Steps']:
            print(f"{datetime.now()} - No active steps found in the cluster")
            return None
            
        # Get the most recent step (steps are returned in reverse chronological order)
        latest_step = response['Steps'][0]
        return latest_step['Id']
        
    except ClientError as e:
        print(f"{datetime.now()} - Error getting latest step: {e}")
        return None
    
def monitor_and_clone_emr_step(cluster_id, region, poll_interval=30, max_retries=3):
    """
    Monitors the latest EMR step and clones the step if it fails or is cancelled.
    """
    emr_client = boto3.client('emr', region_name=region)
    retry_count = 0
    
    # Get the latest step ID to monitor
    step_id = get_latest_step(emr_client, cluster_id)
    if not step_id:
        print(f"{datetime.now()} - No step to monitor. Exiting.")
        return
        
    print(f"{datetime.now()} - Starting to monitor step ID: {step_id}")
    
    while True:
        try:
            # Get the step's current status
            step_details = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
            step_config = step_details['Step']
            step_status = step_config['Status']['State']
            
            print(f"{datetime.now()} - Step {step_id} is currently in status: {step_status}")
            
            if step_status in ["FAILED", "CANCELLED"]:
                if retry_count >= max_retries:
                    print(f"{datetime.now()} - Maximum retry attempts ({max_retries}) reached. Stopping monitoring.")
                    return
                
                print(f"{datetime.now()} - Step has {step_status.lower()}. Attempting to clone the step...")
                
                # Create new step configuration
                new_step_config = create_step_config(step_config)
                
                try:
                    # Add the new step
                    new_step_response = emr_client.add_job_flow_steps(
                        JobFlowId=cluster_id,
                        Steps=[new_step_config]
                    )
                    new_step_id = new_step_response['StepIds'][0]
                    print(f"{datetime.now()} - Step cloned successfully. New Step ID: {new_step_id}")
                    
                    # Update step_id to monitor the new step
                    step_id = new_step_id
                    retry_count += 1
                    print(f"{datetime.now()} - Monitoring the new step ID: {step_id}")
                    
                except ClientError as e:
                    print(f"{datetime.now()} - Error adding new step: {e}")
                    return
                    
            elif step_status in ["COMPLETED", "INTERRUPTED"]:
                print(f"{datetime.now()} - Step has finished with status: {step_status}. Exiting monitor.")
                return
                
            # Wait before polling again
            time.sleep(poll_interval)
            
        except ClientError as e:
            print(f"{datetime.now()} - An error occurred: {e}")
            return
        

if __name__ == "__main__":
    REGION = "us-west-2"              # Replace with your actual AWS region
    MAX_RETRIES = 3                   # Maximum number of retry attempts
    
    monitor_and_clone_emr_step(CLUSTER_ID, REGION, INTERVAL, MAX_RETRIES)
