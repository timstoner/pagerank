package com.example.pagerank.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import com.example.pagerank.io.IntArrayWritable;
import com.example.pagerank.io.PageNodeWritable;

public class PageRankMapper extends
		Mapper<PageNodeWritable, IntArrayWritable, IntWritable, Writable> {

	@Override
	protected void map(PageNodeWritable key, IntArrayWritable value,
			Context context) throws IOException, InterruptedException {
		Writable[] data = value.get();

		float newPageRank = key.getPageRank() / data.length;

		PageNodeWritable sourcePageNode = new PageNodeWritable(key.getPageId(),
				newPageRank);

		IntWritable outgoingPageId;
		for (Writable item : data) {
			outgoingPageId = (IntWritable) item;
			context.write(outgoingPageId, sourcePageNode);
		}

		IntWritable sourcePageId = new IntWritable(key.getPageId());

		context.write(sourcePageId, value);
	}
}
