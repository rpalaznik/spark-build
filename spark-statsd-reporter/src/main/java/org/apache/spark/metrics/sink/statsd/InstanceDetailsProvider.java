package org.apache.spark.metrics.sink.statsd;

import org.apache.http.annotation.NotThreadSafe;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Class used to access SparkEnv instance and extract relevant tags from SparkConf
 * which is shared across Drivers and Executors. SparkEnv initializes Metric Sinks
 * in its constructor is not available in the Sink during initialization (for Executors).
 */
@NotThreadSafe
public class InstanceDetailsProvider  {
    private final static Logger logger = LoggerFactory.getLogger(StatsdReporter.class);

    private Optional<InstanceDetails> instance = null;

    public Optional<InstanceDetails> getInstanceDetails() {
        if(instance == null) {
            if (SparkEnv.get() == null) {
                logger.warn("SparkEnv is not initialized, instance details unavailable");
                instance = Optional.empty();
            } else {
                SparkConf sparkConf = SparkEnv.get().conf();
                instance = Optional.of(
                        new InstanceDetails(
                                sparkConf.getAppId(),
                                sparkConf.get("spark.app.name"),
                                InstanceType.fromString(SparkEnv.get().metricsSystem().instance()),
                                sparkConf.get("spark.executor.id"),
                                sparkConf.get("spark.metrics.namespace", "default")
                        )
                );
            }
        }
        return instance;
    }
}
