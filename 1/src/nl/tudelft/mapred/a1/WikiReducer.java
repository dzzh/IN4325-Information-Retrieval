package nl.tudelft.mapred.a1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WikiReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

}
