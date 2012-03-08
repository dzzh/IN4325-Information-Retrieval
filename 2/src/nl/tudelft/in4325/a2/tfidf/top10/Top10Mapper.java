package nl.tudelft.in4325.a2.tfidf.top10;

import java.io.IOException;

import nl.tudelft.in4325.Constants;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper to retrieve the calculated document scores for each query.
 * 
 */
public class Top10Mapper extends Mapper<Object, Text, Text, Text> {

	private Text word = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] values = value.toString().split("\\s");

		word.set(values[0].trim() + Constants.FIELD_SEPARATOR + values[2].trim());
		context.write(word, new Text(values[1].trim()));

	}
}
