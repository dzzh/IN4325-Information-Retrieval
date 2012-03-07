package nl.tudelft.in4325.a2.tfidf;

import nl.tudelft.in4325.ConfigurationHelper;
import nl.tudelft.in4325.a1.normalization.NormalizationType;
import nl.tudelft.in4325.a2.utils.QueryParser;
import nl.tudelft.in4325.a2.utils.WordIndexExtractor;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

public class TFIDFMapper extends Mapper<Object, Text, Text, Text> {

	private final int numberOfDocuments;
	private final Map<String, Map<String, Integer>> queries;
    private final WordIndexExtractor wordIndexExtractor = new WordIndexExtractor();

	public TFIDFMapper(){
		Configuration appConfig = new ConfigurationHelper().getConfiguration();
        String platform = appConfig.getString("target-platform");
		numberOfDocuments = Integer.valueOf(appConfig.getString(platform + "-number-of-documents"));
        String type = appConfig.getString("normalization-type");
        NormalizationType normalizationType = NormalizationType.getNormalizationType(type);
        String queriesFile = appConfig.getString(platform + "-queries-file");
        queries = new QueryParser(normalizationType.getNormalizer()).parseQuery(queriesFile);
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// parsing the document contents
		String stringValue = value.toString();

		String word = stringValue.substring(0, stringValue.indexOf("<")).trim();

		Map<String, Integer> docNumberOfOccurrences = wordIndexExtractor.extractWordFrequencies(stringValue);

		for (String query : queries.keySet()) {
			if (queries.get(query).keySet().contains(word)) {

				// calculate the IDF of the word
				double wordIDF = Math.log(numberOfDocuments / docNumberOfOccurrences.size());

				// calculate the TF.IDF of the query term
				double queryTFIDF = queries.get(query).get(word) * wordIDF;

				for (String doc : docNumberOfOccurrences.keySet()) {

					double documentTFIDF = calculateTFIDF(docNumberOfOccurrences.get(doc), wordIDF);

					if (documentTFIDF != 0) {
						context.write(new Text(query), new Text(word + ","
								+ doc + "," + documentTFIDF + "," + queryTFIDF));
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


}
