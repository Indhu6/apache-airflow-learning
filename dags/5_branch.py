
from airflow.sdk import dag, task


@dag(
    dag_id="branch_dag",
    schedule=None,
    catchup=False,
    tags=["tutorial", "parallel"]
)
def branch_dag():

    # TASK 1: Extract data
    @task.python
    def extract_task(**kwargs):
        print("Extracting the data...")

        ti = kwargs["ti"]

        extracted_data_dict = {
            "api_extracted_data": [1, 2, 3],
            "db_extracted_data": [4, 5, 6],
            "S3_extracted_data": [7, 8, 9],
            "weekend_flag": "false"
        }

        ti.xcom_push(
            key="return_value",
            value=extracted_data_dict
        )

    # TASK 2: Transform API data
    @task.python
    def transform_task_api(**kwargs):
        ti = kwargs["ti"]

        extracted_data = ti.xcom_pull(
            task_ids="extract_task",
            key="return_value"
        )

        api_data = extracted_data["api_extracted_data"]

        print("Transforming API data...")
        transformed_api_data = [i * 10 for i in api_data]

        return transformed_api_data

    # TASK 3: Transform Database data
    @task.python
    def transform_task_db(**kwargs):
        ti = kwargs["ti"]

        extracted_data = ti.xcom_pull(
            task_ids="extract_task",
            key="return_value"
        )

        db_data = extracted_data["db_extracted_data"]

        print("Transforming DB data...")
        transformed_db_data = [i * 100 for i in db_data]

        return transformed_db_data

    # TASK 4: Transform S3 data
    @task.python
    def transform_task_s3(**kwargs):
        ti = kwargs["ti"]

        extracted_data = ti.xcom_pull(
            task_ids="extract_task",
            key="return_value"
        )

        s3_data = extracted_data["S3_extracted_data"]

        print("Transforming S3 data...")
        transformed_s3_data = [i * 1000 for i in s3_data]

        return transformed_s3_data

    # TASK 5: Decider / Branch task
    @task.branch
    def decider_task(**kwargs):
        ti = kwargs["ti"]

        extracted_data = ti.xcom_pull(
            task_ids="extract_task",
            key="return_value"
        )

        weekend_flag = extracted_data["weekend_flag"]

        print(f"Weekend flag: {weekend_flag}")

        if weekend_flag == "true":
            return "no_load_task"
        else:
            return "load_task"

    # TASK 6: Load all transformed data
    @task.bash
    def load_task(**kwargs):
        ti = kwargs["ti"]

        api_data = ti.xcom_pull(
            task_ids="transform_task_api",
            key="return_value"
        )

        db_data = ti.xcom_pull(
            task_ids="transform_task_db",
            key="return_value"
        )

        s3_data = ti.xcom_pull(
            task_ids="transform_task_s3",
            key="return_value"
        )

        return (
            f"echo 'Loaded data: "
            f"API={api_data}, "
            f"DB={db_data}, "
            f"S3={s3_data}'"
        )

    # TASK 7: No Load task
    @task.bash
    def no_load_task(**kwargs):
        print("No data to load on weekend")
        return "echo 'No load task executed'"

    # Define tasks
    extract = extract_task()

    transform_api = transform_task_api()
    transform_db = transform_task_db()
    transform_s3 = transform_task_s3()

    decider = decider_task()

    load = load_task()
    no_load = no_load_task()

    # Define dependencies
    extract >> [transform_api, transform_db, transform_s3]

    [transform_api, transform_db, transform_s3] >> decider

    decider >> [load, no_load]


branch_dag()
