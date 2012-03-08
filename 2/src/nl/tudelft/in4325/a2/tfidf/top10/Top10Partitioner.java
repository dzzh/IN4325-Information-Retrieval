package nl.tudelft.in4325.a2.tfidf.top10;

import nl.tudelft.in4325.Constants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitions tokens in form %i:%s by %i.
 */
public class Top10Partitioner extends Partitioner<Text, Text> {

	public int getPartition(Text key, Text value, int numPartitions) {
		String qID = key.toString().split(Constants.FIELD_SEPARATOR)[0].trim();
		return qID.hashCode() % numPartitions;
	}

}
