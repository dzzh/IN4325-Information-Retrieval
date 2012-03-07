package nl.tudelft.in4325.a1.indexing;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Builds word-level inverted index file based on (word, document id + term positions) pairs.
 * 
 */
public class WordLevelIndexReducer extends
		Reducer<Text, TextArrayWritable, Text, Text> {
	public void reduce(Text key, Iterable<TextArrayWritable> values,
			Context context) throws IOException, InterruptedException {

		//The page IDs has to be sorted
		Map<Integer, Writable> toReturn = new TreeMap<Integer, Writable>();
		while (values.iterator().hasNext()) {
			Writable[] value = values.iterator().next().get();
			toReturn.put(Integer.valueOf(value[0].toString()), value[1]);
		}

		//constructs the output
		StringBuilder index = new StringBuilder("< ");
		for (Integer pageID : toReturn.keySet()) {
			index.append(pageID);
			index.append(toReturn.get(pageID));
			index.append(";");
		}
		index.append(" >");

		context.write(key, new Text(index.toString()));
	}

}
