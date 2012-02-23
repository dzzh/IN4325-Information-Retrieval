package nl.tudelft.in4325.a1.reporting;

import java.io.File;

import nl.tudelft.in4325.a1.Constants;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Runs two Hadoop jobs to analyze aggregation and term frequency data for Wikipedia corpus.
 */
public class NormalizationReportDriver {
		
	public static void main(String[] args) throws Exception{

		//reading input and output paths from configuration file
		String propertiesPath = Constants.DEFAULT_PROPERTIES_FILE;
		
		if (new File(Constants.PROPERTIES_FILE).exists()){
			propertiesPath = Constants.PROPERTIES_FILE;
		}
		
		Configuration propertiesConfig = null;
		propertiesConfig = new PropertiesConfiguration(propertiesPath);
		String input = propertiesConfig.getString("normalization-output");
		String aggregationOutput = propertiesConfig.getString("normalization-report-aggregation-output");
		String frequencyOutput = propertiesConfig.getString("normalization-report-frequency-output");
	
		//configuring Hadoop and running the jobs
		org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
		
		//Aggregation
        Job job = new Job(hadoopConfig, Constants.Jobs.NORMALIZATION_REPORT_AGGREGATION.toString());
        job.setJarByClass(NormalizationReportDriver.class);
        job.setMapperClass(NormalizationReportAggregationMapper.class);
        job.setReducerClass(NormalizationReportReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
        Path outputPath = new Path(aggregationOutput); 
        FileOutputFormat.setOutputPath(job, outputPath);
        
        FileSystem dfs = FileSystem.get(outputPath.toUri(), hadoopConfig);
        if (dfs.exists(outputPath)) {
        	dfs.delete(outputPath, true);
        }

        job.waitForCompletion(true);
        
        //Frequency distribution
        job = new Job(hadoopConfig, Constants.Jobs.NORMALIZATION_REPORT_FREQUENCY.toString());
        job.setJarByClass(NormalizationReportDriver.class);
        job.setMapperClass(NormalizationReportFrequencyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setSortComparatorClass(FrequencyComparator.class);
        job.setPartitionerClass(FrequencyTermPartitioner.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
        outputPath = new Path(frequencyOutput); 
        FileOutputFormat.setOutputPath(job, outputPath);
        
        dfs = FileSystem.get(outputPath.toUri(), hadoopConfig);
        if (dfs.exists(outputPath)) {
        	dfs.delete(outputPath, true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
