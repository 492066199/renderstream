package com.ubuve.system;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;

import com.ubuve.render.HttpHandle;
/**
 * this is a syn http client
 * we use http keepalive connection
 * if you want asyn http clinet, please connect the auther below
 * @author yangyang21
 *
 */
public class HttpSystemProducer implements SystemProducer{
	private boolean get;
	private String uri;
	private String args;
	private CloseableHttpClient httpclient; 
	private HttpPost httppost;
	private HttpGet httpget;	
	private HttpSystemProducerHandle httpSystemProducerHandle;
	
	public HttpSystemProducerHandle getHttpSystemProducerHandle() {
		return httpSystemProducerHandle;
	}

	public void setHttpSystemProducerHandle(
			HttpSystemProducerHandle httpSystemProducerHandle) {
		this.httpSystemProducerHandle = httpSystemProducerHandle;
	}

	public HttpSystemProducer(String uri, String args, boolean get) {
		this.get = get;
		this.args = args;
		this.uri = uri;
        RequestConfig defaultRequestConfig = RequestConfig.custom()
            .setSocketTimeout(1000)
            .setConnectTimeout(1000)
            .setConnectionRequestTimeout(1000)
            .build();   
        this.httpclient = HttpClients.custom()
            .setDefaultRequestConfig(defaultRequestConfig)
            .build();
	}

	public void start() {
		
	}

	public void stop() {
	
	}

	public void register(String source) {
		
	}

	public void send(String source, OutgoingMessageEnvelope envelope) {
		CloseableHttpResponse response = null;
		if(this.get){
			String tmpUri = httpSystemProducerHandle.handleGetUrl(this.uri, args, envelope.getMessage());
			if(this.httpget == null){
				this.httpget = new HttpGet();
			}
			this.httpget.setURI(URI.create(tmpUri));
			try {
				response = httpclient.execute(this.httpget);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}else {
			if(this.httppost == null){
				httppost = new HttpPost();
			}
			String tmpUri = httpSystemProducerHandle.handlePostUrl(this.uri, args, envelope.getMessage());
			httppost.setURI(URI.create(tmpUri));
			String body = httpSystemProducerHandle.handlePostBody(args, envelope.getMessage());
			InputStreamEntity reqEntity = new InputStreamEntity(
                    new ByteArrayInputStream(body.getBytes()), -1, null);
			this.httppost.setEntity(reqEntity);
			try {
				response = httpclient.execute(this.httppost);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		if(response != null){
            try {
            	HttpEntity entity = response.getEntity();
            	EntityUtils.consume(entity);
            	httpSystemProducerHandle.handlePostResponse(envelope.getMessage(), response);
				response.close();
			} catch (IOException e) {
				e.printStackTrace();
			}    
		}
		
		if(get && this.httpget != null){
			this.httpget.reset();
		}else if(this.httppost != null){	
			this.httppost.reset();
		}
	}

	public void flush(String source) {

	}
	
	public static void main(String[] args) throws InterruptedException {
		HttpSystemProducer hs = new HttpSystemProducer("http://221.179.193.178:33339", "object", false);
		hs.setHttpSystemProducerHandle(new HttpHandle("http://221.179.193.178:33339"));
		while (true) {
			hs.send("test", new OutgoingMessageEnvelope(null, "yangyang21"));
			Thread.sleep(1000L);
		}
	}
}
