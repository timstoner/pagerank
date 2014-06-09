package com.example.pagerank.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.example.pagerank.io.PageRankWritable;

public class PageRankReducer
		extends
		Reducer<IntWritable, PageRankWritable, PageRankWritable, PageRankWritable> {

	private final PageRankWritable outKey = new PageRankWritable();

	private final PageRankWritable defaultValue = new PageRankWritable();

	private float dampingFactor;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		outKey.setNode(true);
		defaultValue.setNode(false);
		Configuration conf = context.getConfiguration();
		dampingFactor = conf.getFloat("dampingFactor", 0.85f);
	}

	@Override
	protected void reduce(IntWritable key, Iterable<PageRankWritable> values,
			Context context) throws IOException, InterruptedException {
		PageRankWritable outgoinglinks = defaultValue;
		float rank = 0;

		for (PageRankWritable value : values) {
			if (value.isNode()) {
				rank += value.getPageRank() * dampingFactor;
			} else {
				outgoinglinks = value;
			}
		}

		rank = (1 - dampingFactor) * rank;

		outKey.setPageId(key.get());
		outKey.setPageRank(rank);

		context.write(outKey, outgoinglinks);
	}
}
