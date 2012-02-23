package nl.tudelft.in4325.a1.reporting;

import java.io.IOException;

import nl.tudelft.in4325.a1.Constants;
import nl.tudelft.in4325.a1.utils.KeyValue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper to sort tokens in decreasing order of frequencies
 */
public class NormalizationReportFrequencyMapper extends Mapper<Object, Text, Text, IntWritable>{

	private Text word = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		KeyValue parsedData = NormalizationReportAggregationMapper.parseLine(value.toString());
		
		word.set(parsedData.getValue() + Constants.FIELD_SEPARATOR + parsedData.getKey());
		context.write(word, new IntWritable(parsedData.getValue()));
	}
}
