package com.example.pagerank.test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
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
		PageRankWritable key = new PageRankWritable();
		key.setNode(true);
		PageRankWritable value = new PageRankWritable();
		value.setNode(false);

		try {
			List<String> lines = FileUtils.readLines(new File(data));
			for (String line : lines) {
				String[] parts = line.split(":");
				String[] header = parts[0].split(",");
				String a = header[0];
				String b = header[1];

				key.setPageId(Integer.parseInt(a));
				key.setPageRank(Float.parseFloat(b));

				if (parts.length > 1) {
					String[] links = parts[1].split(",");
					IntWritable[] elements = new IntWritable[links.length];
					for (int i = 0; i < links.length; i++) {
						int id = Integer.parseInt(links[i]);
						elements[i] = new IntWritable(id);
					}
					value.set(elements);
				} else {
					value.set(new IntWritable[0]);
				}

				mapDriver.addInput(key, value);
			}

			mapDriver.runTest();
		} catch (IOException e) {
			LOG.error("problem running test", e);
		}
	}
}
