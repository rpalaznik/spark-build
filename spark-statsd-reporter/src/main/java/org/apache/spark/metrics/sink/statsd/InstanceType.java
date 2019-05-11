package org.apache.spark.metrics.sink.statsd;

public enum InstanceType {
    DRIVER("driver"),
    EXECUTOR("executor"),
    UNDEFINED("undefined");

    private String value;

    InstanceType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static InstanceType fromString(String value) {
        for (InstanceType instanceType : InstanceType.values()) {
            if (instanceType.value.equalsIgnoreCase(value)) {
                return instanceType;
            }
        }
        throw new IllegalArgumentException("No constant with value " + value + " found");
    }
}
