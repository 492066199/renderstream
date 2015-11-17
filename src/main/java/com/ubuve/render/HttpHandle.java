package com.ubuve.render;

import java.io.Closeable;
import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.util.EntityUtils;

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

	public boolean handleResponse(HttpResponse response) {
		boolean flag = false;
		if(response != null){
			HttpEntity entity = response.getEntity();
			try {
				EntityUtils.consume(entity);
				StatusLine rt = response.getStatusLine();
				if(rt.getStatusCode() == 200){
					flag = true;				
				}
				System.out.println(rt.toString());
				if(response instanceof Cloneable){
					((Closeable) response).close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return flag;
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
