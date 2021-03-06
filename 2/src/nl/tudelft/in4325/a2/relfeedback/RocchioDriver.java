package nl.tudelft.in4325.a2.relfeedback;

import nl.tudelft.in4325.ConfigurationHelper;
import nl.tudelft.in4325.Constants;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RocchioDriver {

    public static void main(String[] args) throws Exception {

        ConfigurationHelper appConfig = new ConfigurationHelper();

        String input = appConfig.getPathDependentString("word-level-index-output");
        String output = appConfig.getPathDependentString("rocchio-output");

        // configuring Hadoop and running the job
        org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();

        // String[] otherArgs = new
        // GenericOptionsParser(hadoopConfig,args).getRemainingArgs();

        // Create TF.IDF vector space
        Job createTFIDFjob = new Job(hadoopConfig, Constants.Jobs.ROCCHIO.name());
        createTFIDFjob.setJarByClass(RocchioDriver.class);
        createTFIDFjob.setMapperClass(RocchioMapper.class);
        createTFIDFjob.setReducerClass(RocchioReducer.class);
        createTFIDFjob.setOutputKeyClass(Text.class);
        createTFIDFjob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(createTFIDFjob, new Path(input));
        Path outputPathTFIDF = new Path(output);
        FileOutputFormat.setOutputPath(createTFIDFjob, outputPathTFIDF);

        FileSystem dfsTFIDF = FileSystem.get(outputPathTFIDF.toUri(), hadoopConfig);
        if (dfsTFIDF.exists(outputPathTFIDF)) {
            dfsTFIDF.delete(outputPathTFIDF, true);
        }

        createTFIDFjob.waitForCompletion(true);
    }
}
