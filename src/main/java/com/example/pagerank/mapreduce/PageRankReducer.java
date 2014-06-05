package com.example.pagerank.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import com.example.pagerank.io.IntArrayWritable;
import com.example.pagerank.io.PageNodeWritable;

public class PageRankReducer extends
		Reducer<IntWritable, Writable, PageNodeWritable, IntArrayWritable> {

	@Override
	protected void reduce(IntWritable key, Iterable<Writable> values,
			Context context) throws IOException, InterruptedException {
		float rank = 0f;
		IntArrayWritable outgoinglinks = null;

		for (Writable value : values) {
			if (value instanceof PageNodeWritable) {
				PageNodeWritable pg = (PageNodeWritable) value;
				rank += pg.getPageRank() * 0.85f;
			} else {
				IntArrayWritable ia = (IntArrayWritable) value;
				outgoinglinks = ia;
			}
		}

		PageNodeWritable page = new PageNodeWritable();
		page.setPageId(key.get());
		page.setPageRank(rank);

		context.write(page, outgoinglinks);
	}

}
