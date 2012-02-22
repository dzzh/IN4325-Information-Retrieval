package nl.tudelft.in4325.a1.normalization;

import java.io.File;

import nl.tudelft.in4325.a1.Constants;
import nl.tudelft.in4325.a1.utils.XmlInputFormat;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SimpleNormalizationDriver {
		
	public static void main(String[] args) throws Exception{

		//reading input and output paths from configuration file
		String propertiesPath = Constants.DEFAULT_PROPERTIES_FILE;
		
		if (new File(Constants.PROPERTIES_FILE).exists()){
			propertiesPath = Constants.PROPERTIES_FILE;
		}
		
		Configuration propertiesConfig = null;
		propertiesConfig = new PropertiesConfiguration(propertiesPath);
		String input = propertiesConfig.getString("source-input");
		String output = propertiesConfig.getString("simple-normalization-output");
	
		//configuring Hadoop and running the job
		org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
		hadoopConfig.set("xmlinput.start","<page>") ;
		hadoopConfig.set("xmlinput.end","</page>") ;
		
        Job job = new Job(hadoopConfig, Constants.Jobs.SIMPLE_NORMALIZATION.toString());
        job.setJarByClass(SimpleNormalizationDriver.class);
        job.setMapperClass(SimpleNormalizationMapper.class);
        job.setReducerClass(SimpleNormalizationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(XmlInputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
        Path outputPath = new Path(output); 
        FileOutputFormat.setOutputPath(job, outputPath);
        
        FileSystem dfs = FileSystem.get(outputPath.toUri(), hadoopConfig);
        if (dfs.exists(outputPath)) {
        	dfs.delete(outputPath, true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
