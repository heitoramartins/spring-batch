package com.jcg.spring.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class AppMain {

	static Job jobObj;
	static JobLauncher jobLauncherObj;
	static ApplicationContext contextObj;
	private static String[] springConfig  = {"spring/batch/jobs/spring-beans.xml" };

	public static void main(String[] args) {
		
		// Loading The Bean Definition From The Spring Configuration File
		contextObj = new ClassPathXmlApplicationContext(springConfig);
		
		jobObj = (Job) contextObj.getBean("helloWorldJob");

		jobLauncherObj = (JobLauncher) contextObj.getBean("jobLauncher");

		try {
			JobExecution execution = jobLauncherObj.run(jobObj, new JobParameters());

			System.out.println("Exit Status : " + execution.getStatus());
		} catch (Exception exceptionObj) {

			exceptionObj.printStackTrace();
		}
		System.out.println("Done");
	}
}