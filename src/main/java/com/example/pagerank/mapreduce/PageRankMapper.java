package com.example.pagerank.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BuildGraphMapper extends
		Mapper<LongWritable, Text, IntWritable, IntWritable> {
	private static final Logger LOG = LoggerFactory
			.getLogger(BuildGraphMapper.class);

	private final IntWritable sourceId = new IntWritable();
	private final IntWritable destiniationId = new IntWritable();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// get text from value
		String text = value.toString();
		// split text string on colon
		String[] pairs = text.split(":");
		// first item is the source page id
		int srcId = Integer.parseInt(pairs[0]);
		// second item is the destination page id
		int desId = Integer.parseInt(pairs[1]);

		sourceId.set(srcId);
		destiniationId.set(desId);

		// emit source id and destination id
		context.write(sourceId, destiniationId);
	}
}
