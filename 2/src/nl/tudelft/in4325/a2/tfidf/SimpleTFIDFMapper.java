package nl.tudelft.in4325.a2.tfidf;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import nl.tudelft.in4325.Constants;
import nl.tudelft.in4325.a2.utils.QueryParser;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleTFIDFMapper extends Mapper<Object, Text, Text, Text> {

	private final int numberOfDocuments;

	private Map<String, Map<String, Integer>> queries;

	private static final Logger LOGGER = LoggerFactory
			.getLogger(SimpleTFIDFMapper.class);

	public SimpleTFIDFMapper() {
		queries = extractQueries();

		String propertiesPath = Constants.DEFAULT_PROPERTIES_FILE;

		if (new File(Constants.PROPERTIES_FILE).exists()) {
			propertiesPath = Constants.PROPERTIES_FILE;
		}

		Configuration propertiesConfig = null;
		try {
			propertiesConfig = new PropertiesConfiguration(propertiesPath);
		} catch (ConfigurationException e) {
			LOGGER.error("Error loading number-of-documents config property");
		}

		numberOfDocuments = Integer.valueOf(propertiesConfig
				.getString("number-of-documents"));
	}

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// parsing the document contents
		String stringValue = value.toString();

		String word = stringValue.substring(0, stringValue.indexOf("<")).trim();

		Map<String, Integer> docNumberOfOccurences = extractIndexInformation(stringValue);

		for (String query : queries.keySet()) {
			if (queries.get(query).keySet().contains(word)) {

				// calculate the IDF of the word
				double wordIDF = Math.log(numberOfDocuments
						/ docNumberOfOccurences.size());

				// calculate the TF.IDF of the query term
				double queryTFIDF = queries.get(query).get(word) * wordIDF;

				for (String doc : docNumberOfOccurences.keySet()) {

					double documentTFIDF = calculateTFIDF(
							docNumberOfOccurences.get(doc), wordIDF);

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
	 * 
	 * @return
	 */
	protected double calculateTFIDF(int frequency, double wordIDF) {
		double documentTFIDF = frequency * wordIDF;
		return documentTFIDF;
	}

	/**
	 * Extracts information about a word from the entry in the index file.
	 * 
	 * @return - Map, where the key is the document ID and the value is the
	 *         number of times that the word occurs within this document.
	 */
	private Map<String, Integer> extractIndexInformation(String stringValue) {
		String content = stringValue.substring(stringValue.indexOf("<") + 1,
				stringValue.lastIndexOf(">"));

		String docs[] = content.split(">;");

		Map<String, Integer> docNumberOfOccurences = new HashMap<String, Integer>();

		for (String doc : docs) {
			if (doc.trim().isEmpty())
				continue;

			int numberOfOccurances = doc.substring(doc.indexOf("<") + 1).split(
					",").length;
			String docId = doc.substring(0, doc.indexOf("<")).trim();
			docNumberOfOccurences.put(docId, numberOfOccurances);
		}
		return docNumberOfOccurences;
	}

	protected Map<String, Map<String, Integer>> extractQueries() {
		return new QueryParser(false).parserQuery(Constants.QUERIES_FILE);
	}

}
