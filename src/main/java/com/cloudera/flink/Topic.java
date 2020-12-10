package com.cloudera.flink;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Topic {
    String name;
    int partitions;
    Properties config;

    private Map<String, String> map = null;

    Topic() {}

    Topic(String name, int partitions, Properties config) {
        this.name = name;
        this.partitions = partitions;
        this.config = config;
    }

    @Override
    public String toString() {
        return String.format("%s(%s, %s, %s)", Topic.class.getSimpleName(),
                name, partitions, config);
    }

    public Map<String, String> getConfigMap() {
        if (map == null) {
            map = new HashMap();
            for (final String name: config.stringPropertyNames())
                map.put(name, config.getProperty(name));
        }
        return map;
    }
}
