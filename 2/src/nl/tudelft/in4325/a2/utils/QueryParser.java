package nl.tudelft.in4325.a2.utils;

import info.bliki.wiki.filter.PlainTextConverter;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;

import nl.tudelft.in4325.a1.normalization.AdvancedNormalizer;

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

	private boolean advancedNormalization;
	
	private AdvancedNormalizer normalizer = new AdvancedNormalizer();

	public QueryParser(boolean advancedNormalization) {
		this.advancedNormalization = advancedNormalization;
	}

	/**
	 * @param Parses
	 *            the file containing the queries.
	 * @return - Map, where the key is the query identifier and the value map,
	 *         where the key is a query term and the value number of occurrences
	 *         of the term in the particular query.
	 */
	public Map<String, Map<String, Integer>> parserQuery(String queryFilePath) {

		Map<String, Map<String, Integer>> queries = new HashMap<String, Map<String, Integer>>();

		try {
			FileInputStream fstream = new FileInputStream(queryFilePath);
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fstream));
			String strLine;
			// Read File Line By Line
			StringBuilder sb = new StringBuilder();
			while ((strLine = br.readLine()) != null) {
				if (strLine.contains(DOC_START_TAG)) {
					sb = new StringBuilder();
				}

				sb.append(strLine);

				if (strLine.contains(DOC_END_TAG)) {
					queries.put(extractDocNum(sb.toString()),
							extractTerms(sb.toString()));
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

		Map<String, Integer> termNumberOfOccurences = new HashMap<String, Integer>();

		String[] terms = normalizeQuery(query);
		for (String term : terms) {
			if (!termNumberOfOccurences.containsKey(term)) {
				termNumberOfOccurences.put(term, 0);
			}

			termNumberOfOccurences.put(term,
					termNumberOfOccurences.get(term) + 1);
		}

		return termNumberOfOccurences;
	}

	private String[] normalizeQuery(String text) {

		if (!advancedNormalization) {
			return text.split("\\s");
		}

		text = text.toLowerCase(Locale.ENGLISH);

		// removing Wikipedia formatting
		String plainText = normalizer.getWikiModel().render(new PlainTextConverter(), text);

		// removing HTML tags
		plainText = normalizer.removeHtmlTags(plainText);

		// additional markup removals
		plainText = normalizer.removeMarkup(plainText);

		StringTokenizer st = new StringTokenizer(plainText, " \t\n\r\f\\/");
		List<String> terms = new ArrayList<String>();

		while (st.hasMoreTokens()) {
			String token = st.nextToken();

			// removing noise symbols (leaving alpha plus couple of others)
			token = normalizer.removeNonAlphaNumericSymbols(token);

			if (normalizer.isTokenValid(token)) {
				// Applying Porter2 stemming algorithm and emit results
				normalizer.getStemmer().setCurrent(token);
				normalizer.getStemmer().stem();
				String word = normalizer.getStemmer().getCurrent();
				terms.add(word);
			}
		}

		return terms.toArray(new String[0]);
	}

	private String extractDocNum(String string) {
		return string.substring(string.indexOf(DOCNO_START_TAG)
				+ DOCNO_START_TAG.length(), string.indexOf(DOCNO_END_TAG));
	}

}
