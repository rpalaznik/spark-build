package org.apache.spark.metrics.sink.statsd;


import com.codahale.metrics.*;
import org.junit.Test;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import static java.lang.Thread.sleep;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class StatsdReporterTest {

    private MetricFormatter testFormatter() {
        InstanceDetailsProvider provider = new InstanceDetailsProviderMock(
                "test-app-01", "Test Spark App",
                InstanceType.DRIVER, "test-instance-01", "default");
        String[] tags = {};
        return new MetricFormatter(provider, "spark", tags);
    }

    @Test
    public void testGauges() throws IOException {
        final int testUdpPort = 4440;
        try (DatagramTestServer server = DatagramTestServer.run(testUdpPort)) {
            StatsdReporter reporter = StatsdReporter
                    .forRegistry(null)
                    .formatter(testFormatter())
                    .host("localhost")
                    .port(testUdpPort)
                    .build();

            SortedMap<String, Gauge> gauges = new TreeMap<>();
            gauges.put("test-app-01.driver.TestGaugeInt", () -> 42);
            gauges.put("test-app-01.driver.TestGaugeFloat", () -> 123.123f);

            reporter.report(gauges, new TreeMap<>(), new TreeMap<>(), new TreeMap<>(), new TreeMap<>());

            pause(100); // Pausing to let DatagramTestServer collect all messages before the assertion

            // Check that both gauge metrics were received and app id is dropped
            assertTrue(server.receivedMessages().stream().allMatch(s -> s.startsWith("spark.driver.testgauge")));
            assertTrue(server.receivedMessages().stream().anyMatch(s -> s.endsWith(":42|g")));
            assertTrue(server.receivedMessages().stream().anyMatch(s -> s.endsWith(":123.12|g")));
            assertEquals(2, server.receivedMessages().size());

        }
    }

    @Test
    public void testCounters() throws IOException {
        final int testUdpPort = 4441;
        try (DatagramTestServer server = DatagramTestServer.run(testUdpPort)) {
            StatsdReporter reporter = StatsdReporter
                    .forRegistry(null)
                    .formatter(testFormatter())
                    .host("localhost")
                    .port(testUdpPort)
                    .build();

            SortedMap<String, Counter> counters = new TreeMap<>();
            Counter counter = new Counter();
            counter.inc();
            counter.inc(2);
            counters.put("test-app-01.driver.TestCounter", counter);

            reporter.report(new TreeMap<>(), counters, new TreeMap<>(), new TreeMap<>(), new TreeMap<>());

            pause(100);

            assertTrue(server.receivedMessages().stream().allMatch(s -> s.startsWith("spark.driver.testcounter")));
            assertTrue(server.receivedMessages().stream().allMatch(s -> s.endsWith(":3|c")));
            assertEquals(1, server.receivedMessages().size());
        }
    }

    @Test
    public void testHistograms() throws IOException {
        final int testUdpPort = 4442;
        try (DatagramTestServer server = DatagramTestServer.run(testUdpPort)) {
            StatsdReporter reporter = StatsdReporter
                    .forRegistry(null)
                    .formatter(testFormatter())
                    .host("localhost")
                    .port(testUdpPort)
                    .build();

            SortedMap<String, Histogram> histograms = new TreeMap<>();
            Histogram histogram = new Histogram(new ExponentiallyDecayingReservoir());
            histogram.update(1);
            histogram.update(2);
            histogram.update(4);
            histogram.update(0);
            histogram.update(6);
            histograms.put("test-app-01.driver.TestHistogram", histogram);

            reporter.report(new TreeMap<>(), new TreeMap<>(), histograms, new TreeMap<>(), new TreeMap<>());

            pause(100);

            String prefix = "spark.driver.testhistogram.";
            assertThatExists(server, prefix, "count", "5|g");
            assertThatExists(server, prefix, "max", "6|ms");
            assertThatExists(server, prefix, "mean", "2.60|ms");
            assertThatExists(server, prefix, "min", "0|ms");
            assertThatExists(server, prefix, "stddev", "2.15|ms");
            assertThatExists(server, prefix, "p50", "2.00|ms");
            assertThatExists(server, prefix, "p75", "4.00|ms");
            assertThatExists(server, prefix, "p95", "6.00|ms");
            assertThatExists(server, prefix, "p98", "6.00|ms");
            assertThatExists(server, prefix, "p99", "6.00|ms");
            assertThatExists(server, prefix, "p999", "6.00|ms");
            assertEquals(11, server.receivedMessages().size());
        }
    }

    @Test
    public void testMeters() throws IOException {
        final int testUdpPort = 4443;
        try (DatagramTestServer server = DatagramTestServer.run(testUdpPort)) {
            StatsdReporter reporter = StatsdReporter
                    .forRegistry(null)
                    .formatter(testFormatter())
                    .host("localhost")
                    .port(testUdpPort)
                    .build();

            SortedMap<String, Meter> meters = new TreeMap<>();
            Meter meter = new Meter();
            meter.mark();
            meter.mark(2);
            meters.put("test-app-01.driver.TestMeter", meter);

            reporter.report(new TreeMap<>(), new TreeMap<>(), new TreeMap<>(), meters, new TreeMap<>());

            pause(100);

            String prefix = "spark.driver.testmeter.";
            assertThatExists(server, prefix, "count", "3|g");
            assertThatExists(server, prefix, "m1_rate", "0.00|ms");
            assertThatExists(server, prefix, "m5_rate", "0.00|ms");
            assertThatExists(server, prefix, "m15_rate", "0.00|ms");
            // mean value depends on the timing and can't be predicted for the test
            assertThatExists(server, prefix, "mean_rate", "|ms");
            assertEquals(5, server.receivedMessages().size());
        }
    }

    @Test
    public void testTimers() throws IOException {
        final int testUdpPort = 4444;
        try (DatagramTestServer server = DatagramTestServer.run(testUdpPort)) {
            StatsdReporter reporter = StatsdReporter
                    .forRegistry(null)
                    .formatter(testFormatter())
                    .host("localhost")
                    .port(testUdpPort)
                    .build();

            SortedMap<String, Timer> timers = new TreeMap<>();
            Timer timer = new Timer();
            Timer.Context timerContext = timer.time();
            pause(100);
            timerContext.stop();
            timers.put("test-app-01.driver.TestTimer", timer);

            reporter.report(new TreeMap<>(), new TreeMap<>(),new TreeMap<>(), new TreeMap<>(), timers);

            pause(100);

            String prefix = "spark.driver.testtimer.";
            assertThatExists(server, prefix, "max", "|ms");
            assertThatExists(server, prefix, "mean", "|ms");
            assertThatExists(server, prefix, "min", "|ms");
            assertThatExists(server, prefix, "stddev", "|ms");
            assertThatExists(server, prefix, "p50", "|ms");
            assertThatExists(server, prefix, "p75", "|ms");
            assertThatExists(server, prefix, "p95", "|ms");
            assertThatExists(server, prefix, "p98", "|ms");
            assertThatExists(server, prefix, "p99", "|ms");
            assertThatExists(server, prefix, "p999", "|ms");
            assertThatExists(server, prefix, "m1_rate", "0.00|ms");
            assertThatExists(server, prefix, "m5_rate", "0.00|ms");
            assertThatExists(server, prefix, "m15_rate", "0.00|ms");
            assertThatExists(server, prefix, "mean_rate", "|ms");
            assertEquals(14, server.receivedMessages().size());
        }
    }

    private void pause(long millis) {
        try {
            sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void assertThatExists(DatagramTestServer server, String expectedMetricNamePrefix, String expectedMetricArgumentName, String expectedValueAndType) {
        assertTrue(server.receivedMessages().stream().anyMatch(
                s -> s.startsWith(expectedMetricNamePrefix + expectedMetricArgumentName)
                        && s.endsWith(expectedValueAndType)));
    }

}