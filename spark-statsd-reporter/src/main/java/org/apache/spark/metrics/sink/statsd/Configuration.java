package org.apache.spark.metrics.sink.statsd;

interface Configuration {
    interface Keys {
        String HOST = "host";
        String PORT = "port";
        String PREFIX = "prefix";
        String TAGS = "tags";
        String POLL_INTERVAL = "poll.interval";
        String POLL_UNIT = "poll.unit";
        String RATE_UNIT = "rate.unit";
        String DURATION_UNIT = "duration.unit";
    }

    interface Defaults {
        String HOST = "127.0.0.1";
        String PORT = "8125";
        String TAGS = "";
        String POLL_INTERVAL = "10";
        String POLL_UNIT = "SECONDS";
        String RATE_UNIT = "SECONDS";
        String DURATION_UNIT = "MILLISECONDS";
        String PREFIX = "";
    }
}
