from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import timedelta
from aws_operators.ecs import get_ecs_task_definition


# DAG Definition
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=180), # 3 hours
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': False,
}

with DAG(
    "run_ingestion_workflow_dag",
    default_args=default_args,
    description="Trigger ECS Fargate Task for Ingestion",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    # ECS Configuration
    PARSER_CONTAINER_NAME = Variable.get("PARSER_CONTAINER_NAME", "ingestion-container")
    EMBEDDING_CONTAINER_NAME = Variable.get("EMBEDDING_CONTAINER_NAME", "ingestion-embedder-container")
    CLUSTER_NAME = Variable.get("CLUSTER_NAME", "dev-ingestion-fargate-ecs-cluster")
    PARSER_TASK_DEFINITION = Variable.get("PARSER_TASK_DEFINITION", "dev-ingestion-fargate-ecs-cluster-task-definition:2")
    EMBEDDING_TASK_DEFINITION = Variable.get("EMBEDDING_TASK_DEFINITION", "dev-ingestion-fargate-ecs-cluster-task-definition:4")
    
    parser_command = [
        "python3.11",
        "src/parser/app-cli/file_parser_workflow.py",
        "--config",
        "{{ dag_run.conf | tojson }}",
    ]
    run_parsing_fargate_task = get_ecs_task_definition(
        cluster_name=CLUSTER_NAME,
        task_definition_name=PARSER_TASK_DEFINITION,
        container_name=PARSER_CONTAINER_NAME,
        command=parser_command,
    )
    embedding_command = [
        "python3.11",
        "src/embeddings/app-cli/file_embedder_workflow.py",
        "--config",
        "{{ dag_run.conf | tojson }}",
    ]
    run_embedding_generation_fargate_task = get_ecs_task_definition(
        cluster_name=CLUSTER_NAME,
        task_definition_name=EMBEDDING_TASK_DEFINITION,
        container_name=EMBEDDING_CONTAINER_NAME,
        command=embedding_command,
    )


    run_parsing_fargate_task >> run_embedding_generation_fargate_task