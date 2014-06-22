package com.example.pagerank;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.pagerank.io.GraphSequenceFileInputFormat;
import com.example.pagerank.io.GraphSequenceFileOutputFormat;
import com.example.pagerank.io.PageRankWritable;
import com.example.pagerank.mapreduce.PageRankMapper;
import com.example.pagerank.mapreduce.PageRankReducer;

public class PageRankDriver extends Configured implements Tool {
	private static final Logger LOG = LoggerFactory
			.getLogger(PageRankDriver.class);

	private static final String INPUT = "input";
	private static final String OUTPUT = "output";
	private static final String FACTOR = "factor";
	private static final String ITERATIONS = "iterations";
	private static final String REDUCERS = "reducers";

	private String inputPath;

	private String outputPath;

	private float dampingFactor = 0.15f;

	private int numberOfIterations = 5;

	private int numberOfReducers = 1;

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PageRankDriver(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		LOG.info("PAGE RANK!");
		parseArgs(args);

		DateTime startDateTime = new DateTime();

		String input = inputPath;
		String output = outputPath;

		for (int i = 0; i < numberOfIterations; i++) {
			LOG.info("Running Page Rank Iteration: " + (i + 1) + "/"
					+ numberOfIterations);

			output = outputPath + "/iter" + i;
			runJob(input, output);
			input = output;
		}

		DateTime endDateTime = new DateTime();

		LOG.info("All done!");
		Interval interval = new Interval(startDateTime, endDateTime);
		LOG.info("Total Time: " + interval.toDuration().getStandardSeconds());

		return 0;
	}

	private void runJob(String input, String output) throws IOException,
			ClassNotFoundException, InterruptedException {
		LOG.info("Running Job: input-" + input);
		Configuration conf = getConf();
		conf.setFloat(FACTOR, dampingFactor);

		Job job = Job.getInstance(conf, "Page Rank");
		job.setJobName(PageRankDriver.class.getSimpleName() + " output-"
				+ output);
		job.setJarByClass(PageRankDriver.class);
		job.setNumReduceTasks(numberOfReducers);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setInputFormatClass(GraphSequenceFileInputFormat.class);
		job.setOutputFormatClass(GraphSequenceFileOutputFormat.class);

		job.setOutputKeyClass(PageRankWritable.class);
		job.setOutputValueClass(PageRankWritable.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(PageRankWritable.class);

		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);

		// Delete the output directory if it exists already.
		FileSystem.get(conf).delete(new Path(output), true);

		job.waitForCompletion(true);
	}

	private void parseArgs(String[] args) {
		LOG.info("Parsing Arguments");
		Option inputOption = new Option("i", INPUT, true, "input path");
		Option outputOption = new Option("o", OUTPUT, true, "output path");
		Option factorOption = new Option("f", FACTOR, true, "damping factor");
		Option iterationsOption = new Option("n", ITERATIONS, true,
				"number of iterations");
		Option reducersOption = new Option("r", REDUCERS, true,
				"number of reducers");

		Options options = new Options();
		options.addOption(outputOption);
		options.addOption(inputOption);
		options.addOption(factorOption);
		options.addOption(reducersOption);
		options.addOption(iterationsOption);

		CommandLineParser parser = new BasicParser();
		CommandLine cmdline = null;

		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			LOG.error("Error parsing command line: " + exp.getMessage(), exp);
			System.exit(-1);
		}

		if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
			LOG.info("args: " + Arrays.toString(args));
			HelpFormatter formatter = new HelpFormatter();
			formatter.setWidth(120);
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(-1);
		}

		inputPath = cmdline.getOptionValue(INPUT);
		outputPath = cmdline.getOptionValue(OUTPUT);

		if (cmdline.hasOption(FACTOR)) {
			String s = cmdline.getOptionValue(FACTOR);
			dampingFactor = Float.parseFloat(s);
		}

		if (cmdline.hasOption(ITERATIONS)) {
			String s = cmdline.getOptionValue(ITERATIONS);
			numberOfIterations = Integer.parseInt(s);
		}

		if (cmdline.hasOption(REDUCERS)) {
			String s = cmdline.getOptionValue(REDUCERS);
			numberOfReducers = Integer.parseInt(s);
		}

		LOG.info("Tool name: " + PageRankDriver.class.getSimpleName());
		LOG.info(" - inputDir: " + inputPath);
		LOG.info(" - outputDir: " + outputPath);
		LOG.info(" - dampingFactor: " + dampingFactor);
		LOG.info(" - numberOfIterations: " + numberOfIterations);
		LOG.info(" - numberOfReducers:" + numberOfReducers);

	}
}
