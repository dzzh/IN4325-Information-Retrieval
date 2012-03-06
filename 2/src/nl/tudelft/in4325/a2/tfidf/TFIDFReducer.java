package nl.tudelft.in4325.a2.tfidf;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Calculates the scores of the documents for particular query.
 * 
 */
public class TFIDFReducer extends Reducer<Text, Text, Text, Text> {

	private static final int PRECISION = 10;

	private static final DecimalFormat twoDForm = new DecimalFormat("#.##");

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Map<String, Double> docScore = new HashMap<String, Double>();
		Map<String, Double> docLength = new HashMap<String, Double>();

		while (values.iterator().hasNext()) {

			String[] value = values.iterator().next().toString().split(",");

			String docID = value[value.length - 3];

			Double documentTFIDF = Double.valueOf(value[value.length - 2]);

			Double queryTFIDF = Double.valueOf(value[value.length - 1]);
			
			if (!docScore.containsKey(docID)) {
				docScore.put(docID, 0d);
				docLength.put(docID, 0d);
			}

			docScore.put(docID, docScore.get(docID)
					+ (documentTFIDF * queryTFIDF));

			docLength.put(
					docID,
					docLength.get(docID) + Math.pow(documentTFIDF, 2)
							+ Math.pow(queryTFIDF, 2));

		}

		for (String docID : docLength.keySet()) {
			// scores[D]=scores[D]/length[D]
			docScore.put(docID,
					docScore.get(docID) / Math.sqrt(docLength.get(docID)));
		}

		// The documents has to be sorted based on their score
		TreeMap<String, Double> sorted_map = new TreeMap<String, Double>(
				new ValueComparator(docScore));
		sorted_map.putAll(docScore);

		int counter = 0;
		for (String docId : sorted_map.keySet()) {
			if (counter >= PRECISION)
				break;
			context.write(
					key,
					new Text(docId + " " + twoDForm.format(docScore.get(docId))));
			counter++;
		}
	}
}

class ValueComparator implements Comparator<String> {

	Map<String, Double> base;

	public ValueComparator(Map<String, Double> base) {
		this.base = base;
	}

	public int compare(String a, String b) {
		return ((base.get(b) > base.get(a)) ? 1 : -1);
	}
}
