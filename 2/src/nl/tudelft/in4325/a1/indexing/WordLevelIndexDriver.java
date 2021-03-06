package nl.tudelft.in4325.a1.indexing;

import nl.tudelft.in4325.ConfigurationHelper;
import nl.tudelft.in4325.Constants;
import nl.tudelft.in4325.a1.normalization.NormalizationMapper;
import nl.tudelft.in4325.a1.utils.XmlInputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordLevelIndexDriver {
	
	public static void main(String[] args) throws Exception{

		ConfigurationHelper appConfig = new ConfigurationHelper();
		String input = appConfig.getPlatformDependentString("source-input");
		String output = appConfig.getPathDependentString("word-level-index-output");

		//configuring Hadoop and running the job
		org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
		hadoopConfig.set("xmlinput.start","<page>") ;
		hadoopConfig.set("xmlinput.end","</page>") ;
		
        //String[] otherArgs = new GenericOptionsParser(hadoopConfig,args).getRemainingArgs();
        
        Job job = new Job(hadoopConfig, Constants.Jobs.INVERTED_INDEXING.name());
        job.setJarByClass(WordLevelIndexDriver.class);
        job.setMapperClass(NormalizationMapper.class);
        job.setReducerClass(WordLevelIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TextArrayWritable.class);
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
