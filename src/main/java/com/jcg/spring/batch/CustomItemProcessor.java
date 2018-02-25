package com.jcg.spring.batch;

import org.springframework.batch.item.ItemProcessor;

public class CustomItemProcessor implements ItemProcessor<Report, Report> {

	public Report process(Report itemObj) throws Exception {
		System.out.println("Processing Item?= " + itemObj);
		return itemObj;
	}
}