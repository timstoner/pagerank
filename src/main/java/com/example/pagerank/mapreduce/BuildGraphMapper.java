package com.example.pagerank.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.example.pagerank.io.IntArrayWritable;
import com.example.pagerank.io.PageNodeWritable;

public class BuildGraphMapper extends
		Mapper<LongWritable, Text, PageNodeWritable, IntArrayWritable> {

	private final PageNodeWritable node = new PageNodeWritable();

	private final IntArrayWritable array = new IntArrayWritable();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] pairs = value.toString().split(":");

		int pageid = Integer.parseInt(pairs[0]);
		node.setPageId(pageid);
		node.setPageRank(0.5f);

		String[] linkids = pairs[1].split(",");
		IntWritable[] writables = new IntWritable[linkids.length];

		int i = 0;
		for (String linkid : linkids) {
			int id = Integer.parseInt(linkid);
			writables[i] = new IntWritable(id);
			i++;
		}

		array.set(writables);
		context.write(node, array);
	}
}
