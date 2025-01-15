from unittest.mock import patch
from transform_function import transform_load_data

@patch('transform_function.boto3.client')
def test_transform_load_data(mock_boto_client):
    mock_s3 = mock_boto_client.return_value
    mock_s3.put_object.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}

    # Simulate Airflow's task_instance.xcom_pull
    class MockTaskInstance:
        def xcom_pull(self, task_ids):
            return {
                "name": "Stockholm",
                "weather": [{"description": "clear sky"}],
                "main": {"temp": 300, "feels_like": 298, "temp_min": 295, "temp_max": 305, "pressure": 1013, "humidity": 60},
                "wind": {"speed": 5},
                "dt": 1609459200,
                "timezone": 3600,
                "sys": {"sunrise": 1609480800, "sunset": 1609513200}
            }

    task_instance = MockTaskInstance()
    transform_load_data(task_instance)

    mock_s3.put_object.assert_called_once()
