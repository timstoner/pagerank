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

		readMapperInput();
		readMapperOutput();

		try {
			mapDriver.runTest();
		} catch (IOException e) {
			LOG.error("problem running test", e);
		}
	}

	private void readMapperInput() {
		String data = "src/test/resources/data/pagerankmapperinput.txt";
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
					IntWritable[] elements = splitPageIds(parts[1]);
					value.set(elements);
				} else {
					value.set(new IntWritable[0]);
				}

				mapDriver.addInput(key, value);
			}
		} catch (IOException e) {
			LOG.error("Problem reading input", e);
		}
	}

	private void readMapperOutput() {
		String data = "src/test/resources/data/pagerankmapperoutput.txt";
		IntWritable key = new IntWritable();
		PageRankWritable value = new PageRankWritable();

		try {
			List<String> lines = FileUtils.readLines(new File(data));
			for (String line : lines) {
				String[] parts = line.split(":");
				String a = parts[0];

				key.set(Integer.parseInt(a));

				if (parts.length > 1) {
					if (parts[1].contains(";")) {
						value.setNode(true);
						String[] values = parts[1].split(";");

						int id = Integer.parseInt(values[0]);
						float rank = Float.parseFloat(values[1]);

						value.setPageId(id);
						value.setPageRank(rank);
					} else {
						value.setNode(false);
						IntWritable[] elements = splitPageIds(parts[1]);
						value.set(elements);
					}
				} else {
					value.setNode(false);
					value.set(new IntWritable[0]);
				}

				mapDriver.addOutput(key, value);
			}
		} catch (IOException e) {
			LOG.error("Problem reading output", e);
		}
	}

	private IntWritable[] splitPageIds(String s) {
		String[] links = s.split(",");
		IntWritable[] elements = new IntWritable[links.length];
		for (int i = 0; i < links.length; i++) {
			int id = Integer.parseInt(links[i]);
			elements[i] = new IntWritable(id);
		}
		return elements;
	}
}
