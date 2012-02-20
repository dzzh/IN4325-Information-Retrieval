package nl.tudelft.mapred.wikipedia;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CategoryCountDriver {

	private static final String INPUT = "/user/zmicier/gutenberg";
	private static final String OUTPUT = "/user/zmicier/out0";
	
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        Job job = new Job(conf, "WordCount");
        job.setJarByClass(CategoryCountDriver.class);
        job.setMapperClass(CategoryCountMapper.class);
        job.setReducerClass(CategoryCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(INPUT));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
