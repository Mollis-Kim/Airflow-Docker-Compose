import json
import pathlib

import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG( #  객체의 인스턴스 생성 (모든 워크플로의 시작점)
    dag_id="download_rocket_launches", # (필수) DAG ID
    start_date=airflow.utils.dates.days_ago(14), # (필수) DAG처음 실행 시작 날짜
    schedule_interval="@daily", # DAG 실행 간격
)

download_launches = BashOperator( # Bash오퍼레이터를 이용해 curl로 데이터 다운로드
    task_id="download_launches", # Task ID
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",  # 실행시킬 Bash 커맨드
    dag=dag, # DAG변수 참조
)

# 결과값을 파싱하고 모든 로켓 사진을 다운로드 하는 함수
def _get_pictures():
    # 경로 존재 확인
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

    # launches.json 파일 내용을 파싱하여 이미지 다운로드
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")

# Python 오퍼레이터에서 Python으로 작성된 함수 호출
get_pictures = PythonOperator( 
    task_id="get_pictures", 
    python_callable=_get_pictures, 
    dag=dag,
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    dag=dag,
)

# 태스크 실행순서 설정
download_launches >> get_pictures >> notify