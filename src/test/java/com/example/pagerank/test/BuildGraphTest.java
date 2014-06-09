package com.example.pagerank.test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.pagerank.io.PageRankWritable;
import com.example.pagerank.mapreduce.BuildGraphMapper;

public class BuildGraphTest {
	private static Logger LOG = LoggerFactory.getLogger(BuildGraphTest.class);

	private MapDriver<LongWritable, Text, PageRankWritable, PageRankWritable> mapDriver;

	@Before
	public void before() {
		LOG.info("Running before");

		BuildGraphMapper mapper = new BuildGraphMapper();
		mapDriver = new MapDriver<>(mapper);
	}

	@Test
	public void testMapper() {
		LOG.info("Running testMapper");

		String data = "src/test/resources/data/buildgraphmapper.txt";
		try {
			List<String> lines = FileUtils.readLines(new File(data));

			LongWritable inputKey = new LongWritable(0);
			Text inputValue = new Text();

			for (String line : lines) {
				PageRankWritable outputKey = new PageRankWritable();
				outputKey.setNode(true);
				PageRankWritable outputValue = new PageRankWritable();
				outputValue.setNode(false);

				inputValue.set(line);
				mapDriver.addInput(inputKey, inputValue);

				String[] pairs = line.split(":");
				outputKey.setNode(true);
				outputKey.setPageId(Integer.parseInt(pairs[0]));
				outputKey.setPageRank(0.15f);

				IntWritable[] links;

				if (pairs.length > 1) {
					String[] outIds = pairs[1].split(",");
					List<IntWritable> ids = new ArrayList<>();
					links = new IntWritable[outIds.length];
					for (String outid : outIds) {
						ids.add(new IntWritable(Integer.parseInt(outid)));
					}
					links = ids.toArray(links);
				} else {
					links = new IntWritable[0];
				}

				outputValue.set(links);
				mapDriver.addOutput(outputKey, outputValue);
			}

//			mapDriver.runTest();
		} catch (IOException e) {
			LOG.error("problem running test", e);
		}
	}
}
