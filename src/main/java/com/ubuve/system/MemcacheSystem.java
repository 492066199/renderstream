package com.ubuve.system;

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
public class MemcacheSystem implements SystemFactory{
	
	@Override
	public SystemConsumer getConsumer(String systemName, Config config,
			MetricsRegistry registry) {
		throw new SamzaException(
				"You can't consume data from the memcache interface, instead?");
	}

	@Override
	public SystemProducer getProducer(String systemName, Config config,
			MetricsRegistry registry) {
		String host = config.get("systems." + systemName + ".hosts");
		MemcacheSystemProducer producer = new MemcacheSystemProducer(host);
		return producer;
	}

	@Override
	public SystemAdmin getAdmin(String systemName, Config config) {
		return new SinglePartitionWithoutOffsetsSystemAdmin();
	}

}
