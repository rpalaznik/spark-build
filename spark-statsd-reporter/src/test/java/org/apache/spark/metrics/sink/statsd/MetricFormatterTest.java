package org.apache.spark.metrics.sink.statsd;

import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.apache.spark.metrics.sink.statsd.MetricType.*;



public class MetricFormatterTest {

    @Test
    public void simpleDriverMetricString() {
        InstanceDetailsProvider instanceDetailsProviderMock = new InstanceDetailsProviderMock(
                "834d77b5-a7b1-4c9d-9742-0f95d39d15e0-0002-driver-20190430095449-0001", "Test Spark App",
                InstanceType.DRIVER, "aa31a823-2c7c-40e0-aef9-4b2a42adb461", "default");
        String[] tags = { };
        MetricFormatter formatter = new MetricFormatter(instanceDetailsProviderMock, "spark", tags);

        String actual = formatter.buildMetricString("834d77b5-a7b1-4c9d-9742-0f95d39d15e0-0002-driver-20190430095449-0001.driver.TestSource", "test_metric", 1, GAUGE);

        assertTrue(actual.startsWith("spark.driver.testsource.test_metric,"));
        assertTrue(actual.contains(",spark_app_name=test_spark_app"));
        assertTrue(actual.contains(",spark_instance=driver"));
        assertTrue(actual.contains(",spark_instance_id=aa31a823-2c7c-40e0-aef9-4b2a42adb461"));
        assertTrue(actual.contains(",spark_namespace=default"));
        assertTrue(actual.endsWith(":1|g"));
    }

    @Test
    public void simpleExecutorMetricString() {
        InstanceDetailsProvider instanceDetailsProviderMock = new InstanceDetailsProviderMock(
                "834d77b5-a7b1-4c9d-9742-0f95d39d15e0-0002-driver-20190430095449-0001", "Test Spark App",
                InstanceType.EXECUTOR, "aa31a823-2c7c-40e0-aef9-4b2a42adb461_0", "default");
        String[] tags = { };
        MetricFormatter formatter = new MetricFormatter(instanceDetailsProviderMock, "spark", tags);

        String actual = formatter.buildMetricString("834d77b5-a7b1-4c9d-9742-0f95d39d15e0-0002-driver-20190430095449-0001.aa31a823-2c7c-40e0-aef9-4b2a42adb461_0.TestSource", "test_metric", 1, GAUGE);

        assertTrue(actual.startsWith("spark.executor.testsource.test_metric,"));
        assertTrue(actual.contains(",spark_app_name=test_spark_app"));
        assertTrue(actual.contains(",spark_instance=executor"));
        assertTrue(actual.contains(",spark_instance_id=aa31a823-2c7c-40e0-aef9-4b2a42adb461_0"));
        assertTrue(actual.contains(",spark_namespace=default"));
        assertTrue(actual.endsWith(":1|g"));
    }

    @Test
    public void predefinedTags() {
        InstanceDetailsProvider instanceDetailsProviderMock = new InstanceDetailsProviderMock(
                "834d77b5-a7b1-4c9d-9742-0f95d39d15e0-0002-driver-20190430095449-0001", "Test Spark App",
                InstanceType.DRIVER, "aa31a823-2c7c-40e0-aef9-4b2a42adb461", "default");
        String[] tags = { "foo=bar" };
        MetricFormatter formatter = new MetricFormatter(instanceDetailsProviderMock, "spark", tags);

        String actual = formatter.buildMetricString("834d77b5-a7b1-4c9d-9742-0f95d39d15e0-0002-driver-20190430095449-0001.driver.TestSource", "test_metric", 1, GAUGE);

        assertTrue(actual.contains(",foo=bar"));
    }

    @Test
    public void precisionFormat() {
        InstanceDetailsProvider instanceDetailsProviderMock = new InstanceDetailsProviderMock(
                "834d77b5-a7b1-4c9d-9742-0f95d39d15e0-0002-driver-20190430095449-0001", "Test Spark App",
                InstanceType.DRIVER, "aa31a823-2c7c-40e0-aef9-4b2a42adb461", "default");
        String[] tags = { };
        MetricFormatter formatter = new MetricFormatter(instanceDetailsProviderMock, "spark", tags);

        String actual = formatter.buildMetricString("834d77b5-a7b1-4c9d-9742-0f95d39d15e0-0002-driver-20190430095449-0001.driver.TestSource", "test_metric", 1.1234, GAUGE);

        assertTrue(actual.endsWith(":1.12|g"));
    }
}