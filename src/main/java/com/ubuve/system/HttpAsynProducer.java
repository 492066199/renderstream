package com.ubuve.system;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.ubuve.render.HttpHandle;

public class HttpAsynProducer implements AsynSender{
	private final boolean get;
	private final String uri;
	private final String args;
	private final CloseableHttpAsyncClient httpclient;
	private HttpPost httppost;
	private HttpGet httpget;
	private HttpSystemProducerHandle httpSystemProducerHandle;
	private final List<String> msgs = new ArrayList<String>(15);
	
	public HttpSystemProducerHandle getHttpSystemProducerHandle() {
		return httpSystemProducerHandle;
	}

	public void setHttpSystemProducerHandle(HttpSystemProducerHandle httpSystemProducerHandle) {
		this.httpSystemProducerHandle = httpSystemProducerHandle;
	}
	
	public HttpAsynProducer(String uri, String args, boolean get) {
		this.get = get;
		this.args = args;
		this.uri = uri;
		RequestConfig defaultRequestConfig = RequestConfig.custom()
				.setSocketTimeout(1000)
				.setConnectTimeout(1000)
				.setConnectionRequestTimeout(1000).build();
		System.setProperty("http.keepAlive", "true");
        System.setProperty("http.maxConnections", "20");
		this.httpclient = HttpAsyncClients.custom()
				.useSystemProperties()
				.setDefaultRequestConfig(defaultRequestConfig)
				.build();
		httpclient.start();
	}

	public Future<HttpResponse> asynSendTo(String msg) {
		Future<HttpResponse> response = null;
		if (this.get) {
			String tmpUri = httpSystemProducerHandle.handleGetUrl(this.uri, args, msg);
			if (this.httpget == null) {
				this.httpget = new HttpGet();
			}else {
				this.httpget.reset();
			}
			this.httpget.setURI(URI.create(tmpUri));
			response = httpclient.execute(this.httpget, null);
		} else {
			if (this.httppost == null) {
				httppost = new HttpPost();
			}else {
				this.httppost.reset();
			}
			String tmpUri = httpSystemProducerHandle.handlePostUrl(this.uri, args, msg);
			httppost.setURI(URI.create(tmpUri));
			String body = httpSystemProducerHandle.handlePostBody(args, msg);
			InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(body.getBytes()), -1,
					null);
			this.httppost.setEntity(reqEntity);
			response = httpclient.execute(this.httppost, null);
		}
		return response;
	}
	
	
	@Override
	public void send(String msg) {
		if(msgs.size() < 15){
			msgs.add(msg);
		}else{
			Map<Integer, Future<HttpResponse>> futures = Maps.newHashMap(); 
			ImmutableList<String> cloneList  = ImmutableList.copyOf(msgs);
			for(int i = 0; i < 15; i++){
				Future<HttpResponse> future= asynSendTo(cloneList.get(i));
				futures.put(i, future);
			}
			
			msgs.clear();
			
			Set<Entry<Integer, Future<HttpResponse>>> sets = futures.entrySet();
			for(Entry<Integer, Future<HttpResponse>> pair : sets){
				HttpResponse response = null;
				try {
					response = pair.getValue().get();
					if (!httpSystemProducerHandle.handleResponse(response)) {
						msgs.add(cloneList.get(pair.getKey()));
					}
				} catch (InterruptedException | ExecutionException e) {
					msgs.add(cloneList.get(pair.getKey()));
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void main(String[] args) throws InterruptedException {
		HttpAsynProducer hs = new HttpAsynProducer("http://221.179.193.178:33339", "object", false);
		hs.setHttpSystemProducerHandle(new HttpHandle("http://221.179.193.178:33339"));
		while (true) {
			hs.send("yangyanghaoshuai");
			Thread.sleep(1000L);
		}
	}
}
