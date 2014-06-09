package com.example.pagerank.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.example.pagerank.io.PageRankWritable;

public class BuildGraphMapper extends
		Mapper<LongWritable, Text, PageRankWritable, PageRankWritable> {

	private final PageRankWritable outputKey = new PageRankWritable();

	private final PageRankWritable outputValue = new PageRankWritable();

	private float seed;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		seed = conf.getFloat("seed", 0.5f);

		outputKey.setNode(true);
		outputValue.setNode(false);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] pairs = value.toString().split(":");

		int pageid = Integer.parseInt(pairs[0]);
		outputKey.setPageId(pageid);
		outputKey.setPageRank(seed);

		if (pairs.length > 1) {
			String[] linkids = pairs[1].split(",");
			IntWritable[] writables = new IntWritable[linkids.length];

			int i = 0;
			for (String linkid : linkids) {
				int id = Integer.parseInt(linkid);
				writables[i] = new IntWritable(id);
				i++;
			}
			outputValue.set(writables);
		} else {
			outputValue.set(new IntWritable[0]);
		}

		context.write(outputKey, outputValue);
	}
}
