package nl.tudelft.in4325.a2.tfidf;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calculates the scores of the documents for particular query.
 * 
 */
public class TFIDFReducer extends Reducer<Text, Text, Text, Text> {

	private static final DecimalFormat twoDForm = new DecimalFormat("#.##");

	private static final Logger LOGGER = LoggerFactory
			.getLogger(TFIDFReducer.class);

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		try {

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

				docLength.put(docID,
						docLength.get(docID) + Math.pow(documentTFIDF, 2)
								+ Math.pow(queryTFIDF, 2));

			}

			for (String docID : docLength.keySet()) {
				// scores[D]=scores[D]/length[D]
				docScore.put(docID,
						docScore.get(docID) / Math.sqrt(docLength.get(docID)));
				context.write(
						key,
						new Text(docID + " "
								+ twoDForm.format(docScore.get(docID))));
			}

		} catch (Throwable e) {
			LOGGER.error(e.getMessage(), e);
		}
	}
}
