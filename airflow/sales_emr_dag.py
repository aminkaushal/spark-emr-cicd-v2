from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.utils.dates import days_ago

CLUSTER_ID = 'j-YYYYYYYY'
S3_URI = 's3://your-second-bucket/jobs/sales_report_job.zip'

with DAG(
    dag_id='sales_emr_pipeline',
    default_args={'owner': 'airflow'},
    schedule_interval='@weekly',
    start_date=days_ago(1),
    catchup=False
) as dag:

    add_sales_step = EmrAddStepsOperator(
        task_id='add_sales_step',
        job_flow_id=CLUSTER_ID,
        aws_conn_id='aws_default',
        steps=[{
            'Name': 'Run Sales Report Job',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', '--deploy-mode', 'cluster', S3_URI]
            }
        }]
    )

    watch_sales_step = EmrStepSensor(
        task_id='watch_sales_step',
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='add_sales_step', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

    add_sales_step >> watch_sales_step
