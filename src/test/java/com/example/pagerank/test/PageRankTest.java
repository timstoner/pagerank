package com.example.pagerank.test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.pagerank.io.PageRankWritable;
import com.example.pagerank.mapreduce.PageRankMapper;

public class PageRankTest {
	private static Logger LOG = LoggerFactory.getLogger(PageRankTest.class);

	private MapDriver<PageRankWritable, PageRankWritable, IntWritable, PageRankWritable> mapDriver;

	@Before
	public void before() {
		LOG.info("Running before");

		PageRankMapper mapper = new PageRankMapper();
		mapDriver = new MapDriver<>(mapper);
	}

	@Test
	public void testMapper() {
		LOG.info("Running testMapper");

		String data = "src/test/resources/data/pagerankmapper.txt";
//		try {

//			mapDriver.runTest();
//		} catch (IOException e) {
//			LOG.error("problem running test", e);
//		}
	}
}
