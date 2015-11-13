package com.ubuve.render;

import org.apache.http.client.methods.CloseableHttpResponse;

import com.ubuve.system.HttpSystemProducerHandle;
	
public class HttpHandle implements HttpSystemProducerHandle{
	private final boolean need;
	public HttpHandle(String host) {
		if(host.indexOf('?') > 0){
			/**
			 *  http://10.77.96.56:33333?tty=89 
			 **/
			need = true;
		}else {
			/**
			 *  http://10.77.96.56:33333 
			 **/
			need = false;
		}
	}

	public void handlePostResponse(Object message,
			CloseableHttpResponse response) {
		System.out.println(response.getStatusLine());
		
	}

	public String handlePostUrl(String uri, String args, Object message) {		
		return uri;
	}

	public String handleGetUrl(String uri, String args, Object message) {
		String tmpUri = uri;
		if(need){
			tmpUri = tmpUri + '&' + args + '=' + message;
		}else {
			tmpUri = tmpUri + '?' + args + '=' + message;
		}
		return tmpUri;
	}

	public String handlePostBody(String args, Object message) {
		String body = args + '=' + message;
		return body;
	}
}
