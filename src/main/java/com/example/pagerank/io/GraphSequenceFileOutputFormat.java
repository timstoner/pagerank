package com.example.pagerank.io;

import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class GraphSequenceFileOutputFormat extends
		SequenceFileOutputFormat<PageRankWritable, PageRankWritable> {

}
