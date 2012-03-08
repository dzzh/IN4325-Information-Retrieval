package nl.tudelft.in4325.a2.tfidf.top10;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import nl.tudelft.in4325.Constants;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer to generate report with the documents that scored higher for each query.
 */
public class Top10Reducer extends Reducer<Text, Text, Text, Text>{
	
	private static final int PRECISION = 10;
	
	//FIXME workaround. The problem is that Top10GroupingComparator fails to group the keys correctly.
	private Map<String, Integer> num = new HashMap<String, Integer>();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
//		int counter = 0;
		for(Text text : values){
//			if(counter >= PRECISION){
//				break;
//			}
			
			String queryID = key.toString().split(Constants.FIELD_SEPARATOR)[0];
			String score = key.toString().split(Constants.FIELD_SEPARATOR)[1];
			
			if(!num.containsKey(queryID)){
				num.put(queryID, 0);
			}
			
			if(num.get(queryID) >= PRECISION){
				break;
			}else{
				num.put(queryID, num.get(queryID) + 1);
			}
			
			context.write(new Text(queryID), new Text("Q0 " + text.toString() + " 1 " + score + " Exp"));
			
//			counter++;
		}
    }
}
