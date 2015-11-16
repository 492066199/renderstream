package com.ubuve.system;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class MemcachAsynProducer implements AsynSender{
	private MemcachedClient mClient;
	private String hosts;
	private final ConcurrentLinkedQueue<String> errorQueue = new ConcurrentLinkedQueue<String>();
	private final ConcurrentLinkedQueue<OperationFuture<Boolean>> FutureQueue = new ConcurrentLinkedQueue<OperationFuture<Boolean>>();
	
	public MemcachAsynProducer(String hosts) {
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
	public void send(String msg) {
		if(mClient == null){
			mClient = getClients();
		}
		OperationFuture<Boolean> k = mClient.set("key", 0, "value");
		FutureQueue.add(k);
	}
}
