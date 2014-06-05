package com.example.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.pagerank.io.IntArrayWritable;
import com.example.pagerank.io.PageNodeWritable;
import com.example.pagerank.mapreduce.PageRankMapper;
import com.example.pagerank.mapreduce.PageRankReducer;

public class BuildGraphDriver extends Configured implements Tool {
	private static final Logger LOG = LoggerFactory
			.getLogger(BuildGraphDriver.class);

	private static final String INPUT = "input";
	private static final String OUTPUT = "output";

	private String inputPath;

	private String outputPath;

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new BuildGraphDriver(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		LOG.info("Building the Page Rank Graph");
		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "Build Graph");
		job.setJobName(BuildGraphDriver.class.getSimpleName() + ":" + inputPath);
		job.setJarByClass(BuildGraphDriver.class);
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(PageNodeWritable.class);
		job.setOutputValueClass(IntArrayWritable.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);

		// Delete the output directory if it exists already.
		FileSystem.get(conf).delete(new Path(outputPath), true);

		job.waitForCompletion(true);
		
		return 0;
	}

}
