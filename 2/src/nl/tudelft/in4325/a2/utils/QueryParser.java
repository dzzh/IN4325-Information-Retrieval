package nl.tudelft.in4325.a2.utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.tudelft.in4325.a1.normalization.Normalizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses the file containing the queries.
 * 
 */
public class QueryParser {
	private static final String DOCNO_END_TAG = "</DOCNO>";
	private static final String DOCNO_START_TAG = "<DOCNO>";
	private static final String DOC_END_TAG = "</DOC>";
	private static final String DOC_START_TAG = "<DOC>";
	private static final String TEXT_END_TAG = "</TEXT>";
	private static final String TEXT_START_TAG = "<TEXT>";

	private static final Logger LOGGER = LoggerFactory.getLogger(QueryParser.class);

	private Normalizer normalizer;
	
	public QueryParser(Normalizer normalizer) {
		this.normalizer = normalizer;
	}

	/**
	 * @param queryFilePath
	 *            the file containing the queries.
	 * @return Map, where the key is the query identifier and the value map,
	 *         where the key is a query term and the value number of occurrences
	 *         of the term in the particular query.
	 */
	public Map<String, Map<String, Integer>> parseQueries(String queryFilePath) {

		Map<String, Map<String, Integer>> queries = new HashMap<String, Map<String, Integer>>();

		try {
			FileSystem dfsTFIDF = FileSystem.get(new URI(queryFilePath), new Configuration());
			InputStream fstream = dfsTFIDF.open(new Path(queryFilePath));
			BufferedReader br = new BufferedReader(new InputStreamReader(dfsTFIDF.open(new Path(queryFilePath))));
			String strLine;
			// Read File Line By Line
			StringBuilder sb = new StringBuilder();
			while ((strLine = br.readLine()) != null) {
				if (strLine.contains(DOC_START_TAG)) {
					sb = new StringBuilder();
				}

				sb.append(strLine);

				if (strLine.contains(DOC_END_TAG)) {
					queries.put(extractDocNum(sb.toString()), extractTerms(sb.toString()));
				}
			}
			// Close the input stream
			fstream.close();
		} catch (Exception e) {// Catch exception if any
			LOGGER.error("Error: " + e.getMessage());
			System.exit(1);
		}

		return queries;
	}

	private Map<String, Integer> extractTerms(String string) {
		String query = string.substring(string.indexOf(TEXT_START_TAG)
				+ TEXT_START_TAG.length(), string.indexOf(TEXT_END_TAG));

		Map<String, Integer> termNumberOfOccurrences = new HashMap<String, Integer>();

		Map<String, List<String>> terms = normalizer.normalize(query);
		for (Map.Entry<String, List<String>> term : terms.entrySet()) {
				termNumberOfOccurrences.put(term.getKey(), term.getValue().size());
        }

		return termNumberOfOccurrences;
	}

	private String extractDocNum(String string) {
		return string.substring(string.indexOf(DOCNO_START_TAG)
				+ DOCNO_START_TAG.length(), string.indexOf(DOCNO_END_TAG));
	}

}
