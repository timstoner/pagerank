package com.example.pagerank.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import com.example.pagerank.io.PageRankWritable;

public class PageRankMapper
		extends
		Mapper<PageRankWritable, PageRankWritable, IntWritable, PageRankWritable> {

	private final IntWritable i = new IntWritable();
	private final PageRankWritable node = new PageRankWritable();

	@Override
	protected void map(PageRankWritable key, PageRankWritable value,
			Context context) throws IOException, InterruptedException {
		Writable[] data = value.get();

		float newPageRank = key.getPageRank();

		if (data.length > 0) {
			newPageRank = key.getPageRank() / data.length;
		}

		node.setPageId(key.getPageId());
		node.setPageRank(newPageRank);
		node.setNode(true);

		IntWritable outgoingPageId;
		for (Writable item : data) {
			outgoingPageId = (IntWritable) item;
			context.write(outgoingPageId, node);
		}

		i.set(key.getPageId());

		context.write(i, value);
	}
}
