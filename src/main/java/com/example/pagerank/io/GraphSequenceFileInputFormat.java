package com.example.pagerank.io;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class GraphSequenceFileInputFormat extends
		SequenceFileInputFormat<PageRankWritable, Writable> {

}
