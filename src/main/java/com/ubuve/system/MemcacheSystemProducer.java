package com.ubuve.system;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import net.spy.memcached.MemcachedClient;

import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class MemcacheSystemProducer implements SystemProducer{
	private MemcachedClient mClient;
	private String hosts;
	
	public MemcacheSystemProducer(String hosts) {
		this.hosts = hosts;
		this.mClient = getClients();
	}
	
	
	
	private MemcachedClient getClients() {
		List<InetSocketAddress> sockaddrs = Lists.newLinkedList();
		MemcachedClient m = null;
		List<String> hostaddrs = Lists.newArrayList(Splitter.on(',').omitEmptyStrings().split(this.hosts));
		for(String hostaddr : hostaddrs){
			List<String> addrAndPort = Lists.newArrayList(Splitter.on(':').omitEmptyStrings().split(hostaddr));
			if(addrAndPort.size() == 2){
				sockaddrs.add(new InetSocketAddress(addrAndPort.get(0), Integer.valueOf(addrAndPort.get(1))));
			}
		}
		
		try {
			 m = new MemcachedClient(sockaddrs);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return m;
	}



	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void register(String source) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void send(String source, OutgoingMessageEnvelope envelope) {
		mClient.set("", 0, envelope.getMessage());
	}

	@Override
	public void flush(String source) {
		// TODO Auto-generated method stub
		
	}
}
