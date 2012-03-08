package nl.tudelft.in4325.a2.tfidf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import nl.tudelft.in4325.ConfigurationHelper;
import nl.tudelft.in4325.a1.normalization.NormalizationType;
import nl.tudelft.in4325.a2.utils.QueryParser;

import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TFIDFMapper extends Mapper<Object, Text, Text, Text> {

	private int numberOfDocuments;
	private Map<String, Map<String, Integer>> queries;
    private NormalizationType normalizationType;
	

	public TFIDFMapper(){
		Configuration appConfig = new ConfigurationHelper().getConfiguration();
        String platform = appConfig.getString("target-platform");
		numberOfDocuments = Integer.valueOf(appConfig.getString(platform + "-number-of-documents"));
        String type = appConfig.getString("normalization-type");
        normalizationType = NormalizationType.getNormalizationType(type);
        String queriesFile = appConfig.getString(platform + "-queries-file");
        queries = new QueryParser(normalizationType.getNormalizer()).parseQuery(queriesFile);
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// parsing the document contents
		String stringValue = value.toString();

		String word = stringValue.substring(0, stringValue.indexOf("\t")).trim();

		Map<String, Integer> docNumberOfOccurrences = extractIndexInformation(stringValue);

		for (String query : queries.keySet()) {
			if (queries.get(query).keySet().contains(word)) {

				// calculate the IDF of the word
				double wordIDF = Math.log(numberOfDocuments / docNumberOfOccurrences.size());

				// calculate the TF.IDF of the query term
				double queryTFIDF = queries.get(query).get(word) * wordIDF;

				for (String doc : docNumberOfOccurrences.keySet()) {

					// calculate the TF.IDF of the document term
					double documentTFIDF = calculateTFIDF(docNumberOfOccurrences.get(doc), wordIDF);

					if (documentTFIDF != 0) {
						context.write(new Text(query), new Text(doc + "," + documentTFIDF + "," + queryTFIDF));
					}
				}
			}
		}
	}

	/**
	 * Calculates the tf.idf for each document
	 * @param frequency frequency of term occurrence
     * @param wordIDF IDF of a term                
     * 
	 * @return TF-IDF value of a term
	 */
	protected double calculateTFIDF(int frequency, double wordIDF) {
		return frequency * wordIDF;
	}

	/**
	 * Extracts information about a word from the entry in the index file.
	 * @param stringValue word in index
     * 
	 * @return - Map, where the key is the document ID and the value is the
	 *         number of times that the word occurs within this document.
	 */
	private Map<String, Integer> extractIndexInformation(String stringValue) {
		String docs[] = stringValue.split(" ");

		Map<String, Integer> docNumberOfOccurences = new HashMap<String, Integer>();

		for (int i = 1; i < docs.length; i += 2) {
			docNumberOfOccurences.put(docs[i], Integer.valueOf(docs[i + 1]));
		}
		return docNumberOfOccurences;
	}

}
