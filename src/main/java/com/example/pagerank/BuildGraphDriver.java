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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.pagerank.io.PageRankWritable;
import com.example.pagerank.mapreduce.BuildGraphMapper;

public class BuildGraphDriver extends Configured implements Tool {
	private static final Logger LOG = LoggerFactory
			.getLogger(BuildGraphDriver.class);

	private static final String INPUT = "input";
	private static final String OUTPUT = "output";
	private static final String SEED = "seed";
	private float seed = 0.5f;

	private String inputPath;

	private String outputPath;

	public static void main(String[] args) throws Exception {
		DateTime startDateTime = new DateTime();

		ToolRunner.run(new BuildGraphDriver(), args);
		DateTime endDateTime = new DateTime();
		LOG.info("All done!");
		Interval interval = new Interval(startDateTime, endDateTime);
		LOG.info("Total Time: " + interval.toDuration().getStandardSeconds());
	}

	@Override
	public int run(String[] args) throws Exception {
		LOG.info("Building the Page Rank Graph");
		parseArgs(args);

		Configuration conf = getConf();
		conf.setFloat("seed", seed);

		Job job = Job.getInstance(conf, "Build Graph");
		job.setJobName(BuildGraphDriver.class.getSimpleName() + ":" + inputPath);
		job.setJarByClass(BuildGraphDriver.class);
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(PageRankWritable.class);
		job.setOutputValueClass(PageRankWritable.class);

		job.setMapOutputKeyClass(PageRankWritable.class);
		job.setMapOutputValueClass(PageRankWritable.class);

		job.setMapperClass(BuildGraphMapper.class);

		// Delete the output directory if it exists already.
		FileSystem.get(conf).delete(new Path(outputPath), true);

		job.waitForCompletion(true);

		return 0;
	}

	private void parseArgs(String[] args) {
		LOG.info("Parsing Arguments");
		Option inputOption = new Option("i", "input", true, "input path");
		Option outputOption = new Option("o", "output", true, "output path");
		Option seedOption = new Option("s", "seed", true,
				"initial page rank seed");

		Options options = new Options();
		options.addOption(outputOption);
		options.addOption(inputOption);
		options.addOption(seedOption);

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

		if (cmdline.hasOption(SEED)) {
			String s = cmdline.getOptionValue(SEED);
			seed = Float.parseFloat(s);
		}

		LOG.info("Tool name: " + PageRankDriver.class.getSimpleName());
		LOG.info(" - inputDir: " + inputPath);
		LOG.info(" - outputDir: " + outputPath);
		LOG.info(" - seed: " + seed);
	}

}
