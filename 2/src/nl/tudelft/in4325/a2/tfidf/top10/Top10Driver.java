package nl.tudelft.in4325.a2.tfidf.top10;

import java.io.File;

import nl.tudelft.in4325.Constants;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Runs two Hadoop jobs to extract the documents with the higher score.
 */
public class Top10Driver {

	public static void main(String[] args) throws Exception {

		// reading input and output paths from configuration file
		String propertiesPath = Constants.DEFAULT_PROPERTIES_FILE;

		if (new File(Constants.PROPERTIES_FILE).exists()) {
			propertiesPath = Constants.PROPERTIES_FILE;
		}

		Configuration propertiesConfig = null;
		propertiesConfig = new PropertiesConfiguration(propertiesPath);
		String input = propertiesConfig.getString("tf-idf-output");
		String output = propertiesConfig.getString("top10-output");

		// configuring Hadoop and running the jobs
		org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();

		// Aggregation
		Job job = new Job(hadoopConfig,
				Constants.Jobs.NORMALIZATION_REPORT_FREQUENCY.toString());
		job.setJarByClass(Top10Driver.class);
		job.setMapperClass(Top10Mapper.class);
		job.setReducerClass(Top10Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setGroupingComparatorClass(Top10GroupingComparator.class);
		job.setSortComparatorClass(Top10SortComparator.class);
		job.setPartitionerClass(Top10Partitioner.class);
		

		FileInputFormat.addInputPath(job, new Path(input));
		Path outputPath = new Path(output);
		FileOutputFormat.setOutputPath(job, outputPath);

		FileSystem dfs = FileSystem.get(outputPath.toUri(), hadoopConfig);
		if (dfs.exists(outputPath)) {
			dfs.delete(outputPath, true);
		}

		job.waitForCompletion(true);
	}

}
