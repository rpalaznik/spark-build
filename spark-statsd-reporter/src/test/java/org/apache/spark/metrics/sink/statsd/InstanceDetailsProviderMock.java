package org.apache.spark.metrics.sink.statsd;

import java.util.Optional;

public class InstanceDetailsProviderMock extends InstanceDetailsProvider {

    private InstanceDetails details;

    public InstanceDetailsProviderMock(String applicationId, String applicationName, InstanceType instanceType,
                                       String instanceId, String namespace) {
        details = new InstanceDetails(applicationId, applicationName, instanceType, instanceId, namespace);
    }

    @Override
    public Optional<InstanceDetails> getInstanceDetails() {
        return Optional.of(details);
    }
}
