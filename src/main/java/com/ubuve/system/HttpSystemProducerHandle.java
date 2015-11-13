package com.ubuve.system;

import org.apache.http.client.methods.CloseableHttpResponse;

public interface HttpSystemProducerHandle {

	void handlePostResponse(Object message, CloseableHttpResponse response);

	String handlePostUrl(String uri, String args, Object message);

	String handleGetUrl(String uri, String args, Object message);

	String handlePostBody(String args, Object message);
}
