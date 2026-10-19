from airflow.sdk import dag, task


@dag()
def xcom_auto():

    @task.python
    def first_task():
        print("Extracting Data...This is my First task")
        fetched_data = {"data": [1,2,3,4,5]}
        return fetched_data

    @task.python
    def second_task(data:dict):
        fetched_data = data['data']
        transformed_data = fetched_data * 2
        transformed_data_dict = {"transf data" : transformed_data}
        return transformed_data_dict        

    @task.python
    def third_task(data : dict):
        load_data = data
        return load_data
    
    #defining variables
    first = first_task()
    second = second_task(first)
    third = third_task(second)  

 
xcom_auto()  