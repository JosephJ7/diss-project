
import boto3, json, config
from dagster import asset

emr = boto3.client("emr")          

def _submit_emr_step(script_name: str, step_name: str):
    response = emr.add_job_flow_steps(
        JobFlowId=config.EMR_CLUSTER_ID,
        Steps=[{
            "Name": step_name,
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--py-files", f"s3://{config.CODE_BUCKET}/config.py",
                    f"s3://{config.CODE_BUCKET}/{script_name}"
                ],
            },
        }],
    )
    return response["StepIds"][0]

@asset
def raw_to_bronze_step(context):
    step_id = _submit_emr_step("raw_to_bronze.py", "Raw→Bronze")
    context.log.info(f"Submitted Raw→Bronze step {step_id}")

@asset
def bronze_to_silver_step(context):
    step_id = _submit_emr_step("bronze_to_silver.py", "Bronze→Silver")
    context.log.info(f"Submitted Bronze→Silver step {step_id}")
