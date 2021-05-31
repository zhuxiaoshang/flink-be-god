package org.apache.flink.connector.prometheus;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class PrometheusUtils implements Serializable {
	public static final Logger LOG = LoggerFactory.getLogger(PrometheusUtils.class);

	private static final String BLAH = "flink sql";
	private static final long serialVersionUID = -4390414939098114654L;

	public void pushMetrics(String jobName, String metricsName, Map<String, String> labels, Map<String, String> groups, Double value, String address) throws IOException {
		final CollectorRegistry pushRegistry = new CollectorRegistry();
		PushGateway pg = new PushGateway(address);
		Gauge g;
		if (labels == null) {
			g = Gauge.build().name(metricsName).help(BLAH).register(pushRegistry);
			g.set(value);
		} else {
			g = Gauge.build().name(metricsName).labelNames(labels.keySet().toArray(new String[labels.size()])).help(BLAH).register(pushRegistry);
			g.labels(labels.values().toArray(new String[labels.size()])).set(value);
		}
		if (groups == null) {
			pg.push(pushRegistry, jobName);
		} else {
			pg.push(pushRegistry, jobName, groups);
		}
	}


	public void pushTagMetrics(String jobName, String metricsName, Double value, Map<String, String> groupingKey, String address) throws IOException {
		final CollectorRegistry pushRegistry = new CollectorRegistry();
		PushGateway pg = new PushGateway(address);
		final Gauge g = Gauge.build().name(metricsName).help(BLAH).register(pushRegistry);
		g.set(value);
		pg.push(pushRegistry, jobName, groupingKey);
	}
}
