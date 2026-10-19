from airflow.sdk import dag, task


@dag()
def versioned_dag():

    @task.python
    def first_task():
        print("This is my First task")

    @task.python
    def second_task():
        print("This is my Second task")

    @task.python
    def third_task():
        print("This is my Third task")

    @task.python
    def version_task():
        print("This is my Fourth task")

    first_task() >> second_task() >> third_task() >> version_task()
 
versioned_dag()    

