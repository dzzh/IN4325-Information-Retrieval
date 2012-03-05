package nl.tudelft.in4325.a2.tfidf;

import java.io.File;

import nl.tudelft.in4325.Constants;
import nl.tudelft.in4325.a1.indexing.TextArrayWritable;
import nl.tudelft.in4325.a1.indexing.WordLevelIndexReducer;
import nl.tudelft.in4325.a1.normalization.AdvancedNormalizationMapper;
import nl.tudelft.in4325.a1.utils.XmlInputFormat;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AdvancedSublinearTFIDFDriver {

	// private static final Logger LOGGER =
	// LoggerFactory.getLogger(SimpleNormalizationDriver.class);

	public static void main(String[] args) throws Exception {

		// reading input and output paths from configuration file
		String propertiesPath = Constants.DEFAULT_PROPERTIES_FILE;

		if (new File(Constants.PROPERTIES_FILE).exists()) {
			propertiesPath = Constants.PROPERTIES_FILE;
		}

		Configuration propertiesConfig = null;
		propertiesConfig = new PropertiesConfiguration(propertiesPath);
		String input = propertiesConfig.getString("source-input");
		String outputIndex = propertiesConfig.getString("word-level-index-output");
		String outputTFIDF = propertiesConfig.getString("tf-idf-output");

		// configuring Hadoop and running the job
		org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
		hadoopConfig.set("xmlinput.start", "<page>");
		hadoopConfig.set("xmlinput.end", "</page>");

		// String[] otherArgs = new
		// GenericOptionsParser(hadoopConfig,args).getRemainingArgs();

		Job createIndexJob = new Job(hadoopConfig, Constants.Jobs.ADVANCED_TFIDF.name());
		createIndexJob.setJarByClass(AdvancedSublinearTFIDFDriver.class);
		createIndexJob.setMapperClass(AdvancedNormalizationMapper.class);
		createIndexJob.setReducerClass(WordLevelIndexReducer.class);
		createIndexJob.setOutputKeyClass(Text.class);
		createIndexJob.setOutputValueClass(TextArrayWritable.class);
		createIndexJob.setInputFormatClass(XmlInputFormat.class);

		FileInputFormat.addInputPath(createIndexJob, new Path(input));
		Path outputPath = new Path(outputIndex);
		FileOutputFormat.setOutputPath(createIndexJob, outputPath);

		FileSystem dfs = FileSystem.get(outputPath.toUri(), hadoopConfig);
		if (dfs.exists(outputPath)) {
			dfs.delete(outputPath, true);
		}

		// Create TF.IDF vector space
		Job createTFIDFjob = new Job(hadoopConfig, Constants.Jobs.ADVANCED_TFIDF.name());
		createTFIDFjob.setJarByClass(AdvancedSublinearTFIDFDriver.class);
		createTFIDFjob.setMapperClass(AdvancedSublinearTFIDFMapper.class);
		createTFIDFjob.setReducerClass(TFIDFReducer.class);
		createTFIDFjob.setOutputKeyClass(Text.class);
		createTFIDFjob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(createTFIDFjob, outputPath);
		Path outputPathTFIDF = new Path(outputTFIDF);
		FileOutputFormat.setOutputPath(createTFIDFjob, outputPathTFIDF);

		FileSystem dfsTFIDF = FileSystem.get(outputPathTFIDF.toUri(),
				hadoopConfig);
		if (dfsTFIDF.exists(outputPathTFIDF)) {
			dfsTFIDF.delete(outputPathTFIDF, true);
		}

		createIndexJob.waitForCompletion(true);
		createTFIDFjob.waitForCompletion(true);
	}

}
