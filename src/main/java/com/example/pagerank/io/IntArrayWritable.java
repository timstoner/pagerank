package com.example.pagerank.io;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IntArrayWritable extends ArrayWritable {

	public IntArrayWritable(Writable[] values) {
		super(IntWritable.class, values);
	}

	public IntArrayWritable() {
		super(IntWritable.class);
	}

	public int[] convert() {
		Writable[] data = get();
		int[] output = new int[data.length];

		IntWritable iw;
		for (int i = 0; i < data.length; i++) {
			iw = (IntWritable) data[i];
			output[i] = iw.get();
		}

		return output;
	}
}
