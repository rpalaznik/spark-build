package org.apache.spark.metrics.sink.statsd;


import com.codahale.metrics.*;
import org.junit.Test;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import static junit.framework.TestCase.assertTrue;

public class StatsdReporterTest {

    public static final int TEST_UDP_PORT = 4444;

    private MetricFormatter testFormatter() {
        InstanceDetailsProvider provider = new InstanceDetailsProviderMock(
                "test-app-01", "Test Spark App",
                InstanceType.DRIVER, "test-instance-01", "default");
        String[] tags = {};
        return new MetricFormatter(provider, "spark", tags);
    }

    @Test
    public void testGauges() throws IOException {
        try (DatagramTestServer server = DatagramTestServer.run(TEST_UDP_PORT)) {
            StatsdReporter reporter = StatsdReporter
                    .forRegistry(null)
                    .formatter(testFormatter())
                    .host("localhost")
                    .port(TEST_UDP_PORT)
                    .build();


            SortedMap<String, Gauge> gauges = new TreeMap<String, Gauge>();
            SortedMap<String, Counter> counters = new TreeMap<String, Counter>();
            SortedMap<String, Histogram > histograms = new TreeMap<String, Histogram>();
            SortedMap<String, Meter > meters = new TreeMap<String, Meter>();
            SortedMap<String, Timer > timers = new TreeMap<String, Timer>();

            Gauge<Integer> testGaugeInt = () -> 42;
            gauges.put("test-app-01.driver.TestGaugeInt", testGaugeInt);
            Gauge<Float> testGaugeFloat = () -> 123.123f;
            gauges.put("test-app-01.driver.TestGaugeFloat", testGaugeFloat);

            reporter.report(gauges, counters, histograms, meters, timers);
            for (String message : server.receivedMessages()) {
                System.out.println(message);
            }

            assertTrue(server.receivedMessages().stream().anyMatch(s -> s.endsWith(":42|g")));
            assertTrue(server.receivedMessages().stream().anyMatch(s -> s.endsWith(":123.12|g")));
        }
    }

}