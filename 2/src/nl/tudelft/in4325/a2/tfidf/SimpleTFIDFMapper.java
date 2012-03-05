package nl.tudelft.in4325.a2.tfidf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import nl.tudelft.in4325.Constants;
import nl.tudelft.in4325.a2.utils.QueryParser;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class SimpleTFIDFMapper extends Mapper<Object, Text, Text, Text> {

	// TODO - calculate the number of documents in the corpus.
	private static final int NUMBER_OF_DOCUMENTS = 3000;

//	private static final Logger LOGGER = LoggerFactory
//			.getLogger(SimpleTFIDFMapper.class);

	private Map<String, Map<String, Integer>> queries;

	public SimpleTFIDFMapper() {
		queries = extractQueries();
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
				double wordIDF = Math.log10(NUMBER_OF_DOCUMENTS
						/ docNumberOfOccurences.size());

				// calculate the TF.IDF of the query term
				double queryTFIDF = queries.get(query).get(word) * wordIDF;

				for (String doc : docNumberOfOccurences.keySet()) {

					// calculate the tf.idf for each document
					double documentTFIDF = docNumberOfOccurences.get(doc)
							* wordIDF;

					context.write(new Text(query), new Text(word + "," + doc
							+ "," + documentTFIDF * queryTFIDF));
				}
			}
		}
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
