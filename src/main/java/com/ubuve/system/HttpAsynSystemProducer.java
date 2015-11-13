package com.ubuve.system;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.samza.SamzaException;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.util.ExponentialSleepStrategy;
import org.apache.samza.util.KafkaUtil;

public class HttpAsynSystemProducer implements SystemProducer{
	
	//var producer: Producer[Array[Byte], Array[Byte]] = null
	private Map<String, Future<HttpResponse>> latestFuture = new HashMap<String, Future<HttpResponse>>();
    private AtomicBoolean sendFailed = new AtomicBoolean(false);
    private String systemName;
    private CloseableHttpAsyncClient httpclient;
    private HttpPost httppost;
	private HttpGet httpget;
	
	
	ExponentialSleepStrategy retryBackoff = new ExponentialSleepStrategy(2.0D, 100L, 10000L);
    //var exceptionThrown: AtomicReference[Exception] = new AtomicReference[Exception]()
    
    public HttpAsynSystemProducer() {
    	 httpclient = HttpAsyncClients.createDefault();
    	 httpclient.start();
	}
	@Override
	public void start() {
		// TODO Auto-generated method stub
		//KafkaSystemProducer
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void register(String source) {
		if(latestFuture.containsKey(source)) {
		      throw new SamzaException(String.format("%s is already registered with the %s system producer" , source, this.systemName));
		}
		latestFuture.put(source, null);
	}

	@Override
	public void send(String source, OutgoingMessageEnvelope envelope) {
		try {         
            HttpGet request = new HttpGet("http://www.apache.org/");
            Future<HttpResponse> future = httpclient.execute(request, null);
            HttpResponse response = future.get();
            System.out.println("Response: " + response.getStatusLine());
            System.out.println("Shutting down");
        } finally {
            httpclient.close();
        }
        System.out.println("Done");
		
        //===========================================================================================
    	if(httpget == null){
    		httpget = new HttpGet();
    	}
        httpget.setURI(URI.create(""));
        sendFailed.set(false);
        retryBackoff.run(loopOperation, onException);
        //===========================================================================================
	    //trace("Enqueueing message: %s, %s." format (source, envelope))
	    if(producer == null) {
	      info("Creating a new producer for system %s." format systemName)
	      producer = getProducer()
	      debug("Created a new producer for system %s." format systemName)
	    }
	    // Java-based Kafka producer API requires an "Integer" type partitionKey and does not allow custom overriding of Partitioners
	    // Any kind of custom partitioning has to be done on the client-side
	    val topicName = envelope.getSystemStream.getStream
	    val partitions: java.util.List[PartitionInfo]  = producer.partitionsFor(topicName)
	    val partitionKey = if(envelope.getPartitionKey != null) KafkaUtil.getIntegerPartitionKey(envelope, partitions) else null
	    val record = new ProducerRecord(envelope.getSystemStream.getStream,
	                                    partitionKey,
	                                    envelope.getKey.asInstanceOf[Array[Byte]],
	                                    envelope.getMessage.asInstanceOf[Array[Byte]])

	    sendFailed.set(false)

	    retryBackoff.run(
	      loop => {
	        if(sendFailed.get()) {
	          throw exceptionThrown.get()
	        }
	        val futureRef: Future[RecordMetadata] =
	          producer.send(record, new Callback {
	            def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
	              if (exception == null) {
	                //send was successful. Don't retry
	                metrics.sendSuccess.inc
	              }
	              else {
	                //If there is an exception in the callback, it means that the Kafka producer has exhausted the max-retries
	                //Hence, fail container!
	                exceptionThrown.compareAndSet(null, exception)
	                sendFailed.set(true)
	              }
	            }
	          })
	        latestFuture.put(source, futureRef)
	        metrics.sends.inc
	        if(!sendFailed.get())
	          loop.done
	      },
	      (exception, loop) => {
	        if(exception != null && !exception.isInstanceOf[RetriableException]) {   // Exception is thrown & not retriable
	          debug("Exception detail : ", exception)
	          //Close producer
	          stop()
	          producer = null
	          //Mark loop as done as we are not going to retry
	          loop.done
	          metrics.sendFailed.inc
	          throw new SamzaException("Failed to send message. Exception:\n %s".format(exception))
	        } else {
	          warn("Retrying send messsage due to RetriableException - %s. Turn on debugging to get a full stack trace".format(exception))
	          debug("Exception detail:", exception)
	          metrics.retries.inc
	        }
	      }
	    )
	}

	@Override
	public void flush(String source) {
		// TODO Auto-generated method stub
		
	}
}
