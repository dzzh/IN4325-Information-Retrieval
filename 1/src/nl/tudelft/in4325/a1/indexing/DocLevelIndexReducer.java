package nl.tudelft.in4325.a1.indexing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Builds document-level inverted index file based on (word, document id) pairs.
 *
 */
public class DocLevelIndexReducer extends Reducer<Text, TextArrayWritable, Text, Text> {
	public void reduce(Text key, Iterable<TextArrayWritable> values, Context context)
			throws IOException, InterruptedException {

		List<Integer> toReturn = new ArrayList<Integer>();
		while (values.iterator().hasNext()) {
			//we are only interested in the pageID. The possitions are ignored.
			Text pageID  = (Text) ((TextArrayWritable)values.iterator().next()).get()[0];
			toReturn.add(Integer.valueOf(pageID.toString()));
		}
		
		//The document ids arrive in random order and they have to be sorted.
		Collections.sort(toReturn);
		
		context.write(key, new Text(StringUtils.join(toReturn, "|")));
	}

}
