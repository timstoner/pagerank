package com.example.pagerank.test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.pagerank.mapreduce.PageRankMapper;
import com.example.pagerank.mapreduce.PageRankReducer;

public class BuildGraphTest {
	private static Logger LOG = LoggerFactory.getLogger(BuildGraphTest.class);

	private MapDriver<LongWritable, Text, IntWritable, IntWritable> mapDriver;

	private ReduceDriver<IntWritable, IntWritable, Text, Text> reduceDriver;

	MapReduceDriver<LongWritable, Text, IntWritable, IntWritable, Text, Text> mapReduceDriver;

	@Before
	public void before() {
		LOG.info("Running before");

		PageRankMapper mapper = new PageRankMapper();
		PageRankReducer reducer = new PageRankReducer();

		// mapDriver = new MapDriver<>(mapper);
		reduceDriver = new ReduceDriver<>(reducer);
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, IntWritable, IntWritable, Text, Text>();
	}

	@Test
	public void testMapper() {
		LOG.info("Running testMapper");

		String data = "src/test/resources/data/buildgraphmapper.txt";
		try {
			List<String> lines = FileUtils.readLines(new File(data));
			LongWritable key = new LongWritable(0);
			IntWritable src = new IntWritable();
			IntWritable dest = new IntWritable();

			for (String line : lines) {
				// add each line as input to mapper
				Text value = new Text(line);
				mapDriver.addInput(key, value);

				// split each line to verify output
				String[] pairs = line.split(":");
				src.set(Integer.parseInt(pairs[0]));
				dest.set(Integer.parseInt(pairs[1]));

				mapDriver.addOutput(src, dest);
			}

			mapDriver.runTest();
		} catch (IOException e) {
			LOG.error("problem running test", e);
		}
	}

	@Test
	public void testReducer() {
		LOG.info("Running testReducer");

		String data = "src/test/resources/data/buildgraphreducer.txt";
		try {
			List<String> lines = FileUtils.readLines(new File(data));

			IntWritable key = new IntWritable();

			Text resultKey = new Text();
			Text resultValue = new Text();

			for (String line : lines) {
				String[] pairs = line.split(":");
				key.set(Integer.parseInt(pairs[0]));
				resultKey.set(pairs[0]);

				String[] destIds = pairs[1].split(",");
				List<IntWritable> values = new ArrayList<>();
				for (String id : destIds) {
					IntWritable value = new IntWritable(Integer.parseInt(id));
					values.add(value);
				}
				reduceDriver.addInput(key, values);

				resultValue.set(pairs[1]);

				reduceDriver.addOutput(resultKey, resultValue);
			}

			reduceDriver.runTest();

		} catch (IOException e) {
			LOG.error("problem running test", e);
		}
	}
}
