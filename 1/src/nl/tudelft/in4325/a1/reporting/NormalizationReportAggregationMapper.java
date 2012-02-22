package nl.tudelft.in4325.a1.reporting;

import java.io.IOException;

import nl.tudelft.in4325.a1.Constants;
import nl.tudelft.in4325.a1.utils.KeyValue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NormalizationReportAggregationMapper extends Mapper<Object, Text, Text, IntWritable>{

	private Text word = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		KeyValue parsedData = parseLine(value.toString());
		
		//Mapping to compute number of unique terms
		word.set(Constants.Metrics.UNIQUE_TERM.toString());
		context.write(word, new IntWritable(1));
		
		//Mapping to compute number of non-unique terms 
		word.set(Constants.Metrics.NON_UNIQUE_TERM.toString());
		context.write(word, new IntWritable(parsedData.getValue()));
		
		//Mapping to compute number of terms occurring once
		if (parsedData.getValue() == 1){
			word.set(Constants.Metrics.OCCURRING_ONCE.toString());
			context.write(word, new IntWritable(1));
		}
	}
	
	protected static KeyValue parseLine(String line){
		int separatorIndex = line.lastIndexOf("\t");
		String key = line.substring(0,separatorIndex);
		int value = Integer.valueOf(line.substring(separatorIndex + 1));
		return new KeyValue(key, value);
	}
}
