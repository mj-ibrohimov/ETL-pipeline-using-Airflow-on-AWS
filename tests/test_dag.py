import pytest
from airflow.models import DagBag
from extract_data_dag import dag 

def test_dag_loaded():
    assert dag is not None, "DAG is not loaded"
    assert len(dag.tasks) == 3, "DAG does not have the expected number of tasks"

def test_task_dependencies():
    is_weather_api_available = dag.get_task('is_weather_api_available')
    extract_weather_data = dag.get_task('extract_weather_data')
    transform_load_weather_data = dag.get_task('transform_load_weather_data')

    assert is_weather_api_available.downstream_list == [extract_weather_data], \
        "Task 'is_weather_api_available' should have 'extract_weather_data' as downstream."
    assert extract_weather_data.downstream_list == [transform_load_weather_data], \
        "Task 'extract_weather_data' should have 'transform_load_weather_data' as downstream."
