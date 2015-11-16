package com.ubuve.system;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

public class MemcachAsynProducer implements AsynSender{
	private MemcachedClient mClient;
	private String hosts;
	private final List<String> msgs = new ArrayList<String>(20);
	
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
		if(msgs.size() < 15){
			msgs.add(msg);
		}else{
			if(mClient == null){
				mClient = getClients();
			}
			Map<String, OperationFuture<Boolean>> futures = Maps.newHashMap(); 
			for(String tmp : msgs){
				OperationFuture<Boolean> future = mClient.set("key", 0, "value");
				futures.put(tmp, future);
			}
			msgs.clear();
			Set<Entry<String, OperationFuture<Boolean>>> sets = futures.entrySet();
			
			for(Entry<String, OperationFuture<Boolean>> pair : sets){
				try {
					if (!pair.getValue().get()) {
						msgs.add(pair.getKey());
					}
				} catch (InterruptedException | ExecutionException e) {
					msgs.add(pair.getKey());
					e.printStackTrace();
				}
			}
		}
	}
}
