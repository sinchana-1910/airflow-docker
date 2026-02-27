import json
import pathlib
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

IMAGE_DIR = "/tmp/images"

dag = DAG(
    dag_id="download_rocket_launches",
    start_date=datetime.now() - timedelta(days=14),
    schedule=None,
)

download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.json -L https://ll.thespacedevs.com/2.0.0/launch/upcoming",
    dag=dag,
)

def get_pictures():
    pathlib.Path(IMAGE_DIR).mkdir(exist_ok=True)

    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]

    for image_url in image_urls:
        try:
            response = requests.get(image_url)
            image_filename = image_url.split("/")[-1]
            target_file = f"{IMAGE_DIR}/{image_filename}"
            with open(target_file, "wb") as f:
                f.write(response.content)
            print(f"Downloaded {image_url}")
        except requests.exceptions.MissingSchema:
            print(f"Invalid URL: {image_url}")
        except requests.exceptions.ConnectionError:
            print(f"Connection failed: {image_url}")

get_pictures_task = PythonOperator(
    task_id="get_pictures",
    python_callable=get_pictures,
    dag=dag,
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images | wc -l) images."',
    dag=dag,
)

download_launches >> get_pictures_task >> notify