package com.ubuve.render;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

public class RenderStream implements StreamTask {
	public final ConcurrentLinkedQueue<String> errorQueue = new ConcurrentLinkedQueue<String>();
	private final SystemStream OUTPUT_STREAM = new SystemStream("http", "test");
	public void process(IncomingMessageEnvelope envelope,
			MessageCollector collector, TaskCoordinator coordinator)
			throws Exception {
		String msg = (String) envelope.getMessage();
		
		if(msg == null){
			return;
		}
		
		if(msg.indexOf("reallog_mark_ad:3") > -1){
			collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, "reallog_mark_ad"));
		}
	}
}
