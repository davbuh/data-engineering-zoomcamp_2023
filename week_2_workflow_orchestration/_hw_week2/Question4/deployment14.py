from prefect.deployments import Deployment
from etl_web_to_gcs import etl_web_to_gcs
from prefect.filesystems import GitHub 

storage = GitHub.load("github-de2023")

deployment = Deployment.build_from_flow(
     flow=etl_web_to_gcs,
     name="github-example",
     storage=storage,
     entrypoint="week_2_workflow_orchestration/_hw_week2/Question4/etl_web_to_gcs.py:etl_web_to_gcs")

if __name__ == "__main__":
    deployment.apply()