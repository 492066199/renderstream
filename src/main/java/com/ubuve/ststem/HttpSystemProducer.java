package com.ubuve.ststem;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.CodingErrorAction;

import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.HttpConnectionFactory;
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultHttpResponseParserFactory;
import org.apache.http.impl.conn.ManagedHttpClientConnectionFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.conn.SystemDefaultDnsResolver;
import org.apache.http.impl.io.DefaultHttpRequestWriterFactory;
import org.apache.http.io.HttpMessageParserFactory;
import org.apache.http.io.HttpMessageWriterFactory;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;

public class HttpSystemProducer implements SystemProducer{
	private boolean get;
	private String uri;
	private String args;
	private PoolingHttpClientConnectionManager connManager;
	private CloseableHttpClient httpclient; 
	private HttpPost httppost;
	private HttpGet httpget;	
	
	public HttpSystemProducer(String uri, String args, boolean get) {
		this.get = get;
		this.args = args;
		this.uri = uri;
		
        HttpMessageParserFactory<HttpResponse> responseParserFactory = new DefaultHttpResponseParserFactory();
        
        HttpMessageWriterFactory<HttpRequest> requestWriterFactory = new DefaultHttpRequestWriterFactory();
        HttpConnectionFactory<HttpRoute, ManagedHttpClientConnection> connFactory = new ManagedHttpClientConnectionFactory(
                requestWriterFactory, responseParserFactory);

        Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
            .register("http", PlainConnectionSocketFactory.INSTANCE)
            .build();

        this.connManager = new PoolingHttpClientConnectionManager(
                socketFactoryRegistry, connFactory, new SystemDefaultDnsResolver());

        SocketConfig socketConfig = SocketConfig.custom()
            .setTcpNoDelay(true)
            .build();
     
        this.connManager.setDefaultSocketConfig(socketConfig);
        this.connManager.setValidateAfterInactivity(1000);
        
        ConnectionConfig connectionConfig = ConnectionConfig.custom()
            .setMalformedInputAction(CodingErrorAction.IGNORE)
            .setUnmappableInputAction(CodingErrorAction.IGNORE)
            .setCharset(Consts.UTF_8)
            .build();
        
        this.connManager.setDefaultConnectionConfig(connectionConfig);
        this.connManager.setMaxTotal(100);
        this.connManager.setDefaultMaxPerRoute(20);

        RequestConfig defaultRequestConfig = RequestConfig.custom()
            .setExpectContinueEnabled(true)
            .setSocketTimeout(1000)
            .setConnectTimeout(1000)
            .setConnectionRequestTimeout(1000)
            .build();   
        this.httpclient = HttpClients.custom()
                .setConnectionManager(this.connManager)
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
			if(this.httpget == null){
				this.httpget = new HttpGet(this.uri + args + '=' + envelope.getMessage());
			}else {
				this.httpget.setURI(URI.create(this.uri + args + '=' + envelope.getMessage()));
				System.out.println(this.httpget.getURI().toString());
			}
			
			try {
				response = httpclient.execute(this.httpget);
			} catch (Exception e1) {
				e1.printStackTrace();
			} 
		}else {
			if(this.httppost == null){
				httppost = new HttpPost(this.uri);
			}else {
				httppost.setURI(URI.create(this.uri));
			}
			args = args + '=' + envelope.getMessage();
			InputStreamEntity reqEntity = new InputStreamEntity(
                    new ByteArrayInputStream(args.getBytes()), -1, null);
			this.httppost.setEntity(reqEntity);
			try {
				response = httpclient.execute(this.httppost);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		if(response != null){
            HttpEntity entity = response.getEntity();
            
            if (entity != null) {
            	InputStream instream = null;
                try {
                	instream = entity.getContent();
                    instream.read();         
                    instream.close();
                } catch (IOException ex) {
                    
                }
            }
			System.out.println(response.getStatusLine());
			try {
				response.close();
			} catch (IOException e) {
				e.printStackTrace();
			}    
		}
		
		if(get){
			if(this.httpget != null){
				this.httpget.reset();
			}
			if(this.httppost != null){
				this.httppost.reset();
			}
		}
	}

	public void flush(String source) {
		// TODO Auto-generated method stub
	}
	
	public static void main(String[] args) throws InterruptedException {
		HttpSystemProducer hs = new HttpSystemProducer("http://221.179.193.178:33339", "?object", false);
		while (true) {
			hs.send("test", new OutgoingMessageEnvelope(null, "yangyang21"));
		}
	}
}
