package nl.tudelft.mapred.a1;

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikiDriver {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(WikiDriver.class);
	
	private static final String PROPERTIES_FILE = "conf/conf.ini";
	private static final String DEFAULT_PROPERTIES_FILE = "conf/conf.default.ini";
	private static final String JOB = "WordCount";
	
	public static void main(String[] args) throws Exception{

		//reading input and output paths from configuration file
		Configuration propertiesConfig = null;
		String propertiesPath = DEFAULT_PROPERTIES_FILE;
		
		if (new File(PROPERTIES_FILE).exists()){
			propertiesPath = PROPERTIES_FILE;
		}
		
		propertiesConfig = new PropertiesConfiguration(propertiesPath);
		String input = propertiesConfig.getString("input");
		String output = propertiesConfig.getString("output");
	
		//configuring Hadoop and running the job
		org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
        String[] otherArgs = new GenericOptionsParser(hadoopConfig,args).getRemainingArgs();
        Job job = new Job(hadoopConfig, JOB);
        job.setJarByClass(WikiDriver.class);
        job.setMapperClass(WikiMapper.class);
        job.setReducerClass(WikiReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
