from prefect.deployments import Deployment
from parameterized_flow import etl_parent_flow2
from prefect.infrastructure.docker import DockerContainer

docker_block = DockerContainer.load("de-prefect")

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow2,
    name='docker-flow',
    infrastructure=docker_block
)

if __name__ == '__main__':
    docker_dep.apply()