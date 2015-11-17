package com.ubuve.system;

import org.apache.http.HttpResponse;

public interface HttpSystemProducerHandle {

	boolean handleResponse(HttpResponse response);

	String handlePostUrl(String uri, String args, Object message);

	String handleGetUrl(String uri, String args, Object message);

	String handlePostBody(String args, Object message);
}
