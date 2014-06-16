package com.alert.config;

import java.util.Date;

import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleTrigger;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application 
{
	private static final Logger logger = LoggerFactory.getLogger(Application.class);
	private static SchedulerFactory factory = new StdSchedulerFactory();
	private static Scheduler hourScheduler;
	static{
		// 创建一个Scheduler
		try {
			hourScheduler = factory.getScheduler();
			JobDetail jobDetail = new JobDetail("hourStatisticJob", "hourJobGroup",HourJob.class);
			long startTime = System.currentTimeMillis() + 2*1000L;       
			SimpleTrigger trigger = new SimpleTrigger("hourTrigger",       
								                      "hourTriggerGroup",       
								                       new Date(startTime),       
								                       null,       
								                       SimpleTrigger.REPEAT_INDEFINITELY,       
								                       60*60*1000L);      
			hourScheduler.scheduleJob(jobDetail, trigger);
		} catch (SchedulerException e) {
			logger.error("schedule", e);
		}
	}
	
	public void start()
	{
		try {
			hourScheduler.start();
		} catch (SchedulerException e) {
			logger.error("start schedule", e);
		}
	}
	
	public void stop()
	{
		try {
			if(!hourScheduler.isShutdown())
				hourScheduler.shutdown();
		} catch (SchedulerException e) {
			logger.error("stop schedule", e);
		}
	}
	
	public static void main(String[] args) 
	{
		Application app = new Application();
		app.start();
	}
}
