package nl.tudelft.in4325.a1.normalization;

import java.io.IOException;

import nl.tudelft.in4325.a1.indexing.TextArrayWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reducer for Wikipedia XML corpus normalization
 */
public class NormalizationReducer extends Reducer<Text, TextArrayWritable, Text, IntWritable> {
	
	private final Logger LOGGER = LoggerFactory.getLogger(NormalizationReducer.class);

	public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException{
		int sum = 0;
		while (values.iterator().hasNext()) {
			Writable[] value = ((TextArrayWritable) values.iterator().next()).get();
			Text array = (Text) value[1];
			sum += array.toString().split(",").length;
			//LOGGER.info(key + "   " + sum);
		}
		
		context.write(key, new IntWritable(sum));
    }
}
