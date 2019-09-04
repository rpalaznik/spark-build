import logging
import os
import pytest
import spark_utils as utils
import json
import subprocess


log = logging.getLogger(__name__)
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
EXAMPLES_JAR_PATH_TEMPLATE = "https://infinity-artifacts.s3-us-west-2.amazonaws.com/spark/spark-examples_{}-2.4.3.jar"


@pytest.mark.sanity
@pytest.mark.smoke
def test_spark_docker_images():

    spark_dists = _read_dists_from_manifest()

    log.info("Testing the following docker distributions: ")
    for dist in spark_dists:
        if not _docker_image_exists(dist['image']):
            pytest.skip("Can't found one of docker images listed in manifest.json in the registry")

    for dist in spark_dists:
        log.info(f"Running a smoke test with {dist['image']}")
        _test_spark_docker_image(dist)


def _read_dists_from_manifest():
    with open(os.path.join(THIS_DIR, '..', 'manifest.json'), 'r') as file:
        manifest = json.load(file)
    return manifest['spark_dist']


def _docker_image_exists(image):
    log.info(f'Checking if image {image} exists in the registry')
    completed = subprocess.run(f"DOCKER_CLI_EXPERIMENTAL=enabled docker manifest inspect {image}")
    return completed.returncode == 0


def _test_spark_docker_image(dist):
    utils.require_spark(additional_options={'service': {'docker-image': dist['image']}})
    example_jar_url = EXAMPLES_JAR_PATH_TEMPLATE.format(dist['scala_version'])

    expected_groups_count = 12000
    num_mappers = 4
    value_size_bytes = 100
    num_reducers = 4

    utils.run_tests(app_url=example_jar_url,
                    app_args=f"{num_mappers} {expected_groups_count} {value_size_bytes} {num_reducers}",
                    expected_output=str(expected_groups_count),
                    args=["--class org.apache.spark.examples.GroupByTest",
                          "--conf spark.executor.cores=1",
                          "--conf spark.cores.max=4",
                          "--conf spark.scheduler.minRegisteredResourcesRatio=1",
                          "--conf spark.scheduler.maxRegisteredResourcesWaitingTime=3m"])

    utils.teardown_spark()

