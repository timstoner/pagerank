package com.example.pagerank;

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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.pagerank.io.GraphSequenceFileInputFormat;
import com.example.pagerank.io.PageRankComparator;
import com.example.pagerank.io.PageRankWritable;

public class SortResultsDriver extends Configured implements Tool {

	private static final String INPUT = "input";
	private static final String OUTPUT = "output";
	private static final Logger LOG = LoggerFactory
			.getLogger(SortResultsDriver.class);

	private String inputPath;

	private String outputPath;

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SortResultsDriver(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		parseArgs(args);

		LOG.info("Sorting Page Rank Results");
		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "Page Rank: Sort Results");

		job.setJarByClass(SortResultsDriver.class);
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass(GraphSequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setSortComparatorClass(PageRankComparator.class);

		job.setOutputKeyClass(PageRankWritable.class);
		job.setOutputValueClass(PageRankWritable.class);

		job.setMapOutputKeyClass(PageRankWritable.class);
		job.setMapOutputValueClass(PageRankWritable.class);

		job.waitForCompletion(true);

		return 0;
	}

	private void parseArgs(String[] args) {
		LOG.info("Parsing Arguments");
		Option inputOption = new Option("i", INPUT, true, "input path");
		Option outputOption = new Option("o", OUTPUT, true, "output path");

		Options options = new Options();
		options.addOption(outputOption);
		options.addOption(inputOption);

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

		LOG.info("Tool name: " + PageRankDriver.class.getSimpleName());
		LOG.info(" - inputDir: " + inputPath);
		LOG.info(" - outputDir: " + outputPath);
	}
}
