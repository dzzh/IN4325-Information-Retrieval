package nl.tudelft.in4325.a1.reporting;

import java.io.IOException;

import nl.tudelft.in4325.a1.Constants;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer to generate report from normalized Wikipedia corpus
 */
public class NormalizationReportReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	private Text word = new Text();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
	
		//For aggregation mapper
		if (key.toString().equals(Constants.Metrics.UNIQUE_TERM.toString())){
			int numUniqueTerms = 0;
			for (@SuppressWarnings("unused") IntWritable iw : values){
				numUniqueTerms++;
			}
			word.set(Constants.Metrics.UNIQUE_TERM.toString());
			context.write(word, new IntWritable(numUniqueTerms));
		} else if (key.toString().equals(Constants.Metrics.NON_UNIQUE_TERM.toString())){
			int numNonUniqueTerms = 0;
			for (IntWritable iw : values){
				numNonUniqueTerms += iw.get();
			}
			word.set(Constants.Metrics.NON_UNIQUE_TERM.toString());
			context.write(word, new IntWritable(numNonUniqueTerms));
		} else if (key.toString().equals(Constants.Metrics.OCCURRING_ONCE.toString())){
			int numOccurringOnce = 0;
			for (@SuppressWarnings("unused") IntWritable iw : values){
				numOccurringOnce++;
			}
			word.set(Constants.Metrics.OCCURRING_ONCE.toString());
			context.write(word, new IntWritable(numOccurringOnce));
			
		//For frequency distribution
		} else {
			context.write(key, new IntWritable(0));
		}
    }
}
