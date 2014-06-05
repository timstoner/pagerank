package com.example.pagerank.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BuildGraphReducer extends
		Reducer<IntWritable, IntWritable, Text, Text> {

	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		StringBuilder data = new StringBuilder();

		for (IntWritable value : values) {
			data.append(value.get());
			data.append(",");
		}

		Text value = new Text(data.substring(0, data.length() - 1));

		context.write(new Text(key.get() + ""), value);
	}
}
