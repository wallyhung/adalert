package com.alert.config;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HourJob implements Job{
	private static final Logger logger = LoggerFactory.getLogger(HourJob.class);

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException 
	{
		logger.info("hour report reseult validating ....");
		Query.sendMail();
	}

}
