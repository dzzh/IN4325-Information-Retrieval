package nl.tudelft.in4325.a1.reporting;

import nl.tudelft.in4325.a1.Constants;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class FrequencyTermPartitioner extends Partitioner<Text, IntWritable> {

	//private static final Logger LOGGER = LoggerFactory.getLogger(FrequencyTermPartitioner.class);
	
	/**
	 * Accepts keys in form %i:%s, where %i is term frequency for term %s and partitions them by %s.
	 */
	public int getPartition(Text key, IntWritable value, int numPartitions){
		String keyString = key.toString();
		if (keyString.contains(Constants.FIELD_SEPARATOR)){
				int separatorIndex = keyString.indexOf(Constants.FIELD_SEPARATOR);
				String originalValue = keyString.substring(separatorIndex + 1);
				return originalValue.hashCode() % numPartitions;
		}
		else {
			return keyString.hashCode() % numPartitions;
		}
	}
	
}
