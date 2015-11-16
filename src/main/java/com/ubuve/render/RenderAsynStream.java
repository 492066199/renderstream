package com.ubuve.render;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

public class RenderAsynStream implements StreamTask{
	public final ConcurrentLinkedQueue<String> errorQueue = new ConcurrentLinkedQueue<String>();
//	private AsynSender sender = new AsynSender();
	
	@Override
	public void process(IncomingMessageEnvelope envelope,
			MessageCollector collector, TaskCoordinator coordinator)
			throws Exception {
		// TODO Auto-generated method stub
		String msg = (String) envelope.getMessage();
		
		if(msg == null){
			return;
		}
		
		if(msg.indexOf("reallog_mark_ad:3") > -1){
//			sender.send(msg);
		}
	}
	
}
