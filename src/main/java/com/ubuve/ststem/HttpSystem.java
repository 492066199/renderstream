package com.ubuve.ststem;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin;

/**
 * 
 * @author yangyang21
 *
 */
public class HttpSystem implements SystemFactory {
	public SystemConsumer getConsumer(String systemName, Config config,
			MetricsRegistry registry) {
		throw new SamzaException(
				"You can't consume data the http interface, instead?");
	}

	public SystemProducer getProducer(String systemName, Config config,
			MetricsRegistry registry) {
		String host = config.get("systems." + systemName + ".url");
		String args = config.get("systems." + systemName + ".args");
		boolean get = true;
		try {
			get = Boolean.parseBoolean(config.get("systems." + systemName
					+ ".get"));
		} catch (Exception e) {
			get = false;
		}

		return new HttpSystemProducer(host, args, get);
	}

	public SystemAdmin getAdmin(String systemName, Config config) {
		return new SinglePartitionWithoutOffsetsSystemAdmin();
	}
}
