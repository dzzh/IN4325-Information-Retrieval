package nl.tudelft.in4325.a1.normalization;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Advanced implementation of {@link Normalizer}. This normalization technique
 * is used by Claudia when creating the indexes.
 */
public class ClaudiaAdvancedNormalizer implements Normalizer {

	private HashSet<String> stopwords;

	public ClaudiaAdvancedNormalizer() {
		stopwords = new HashSet<String>();
		/*
		 * a very small stopword list
		 */
		stopwords.add("a");
		stopwords.add("an");
		stopwords.add("are");
		stopwords.add("as");
		stopwords.add("at");
		stopwords.add("be");
		stopwords.add("by");
		stopwords.add("for");
		stopwords.add("from");
		stopwords.add("how");
		stopwords.add("in");
		stopwords.add("is");
		stopwords.add("it");
		stopwords.add("of");
		stopwords.add("on");
		stopwords.add("or");
		stopwords.add("that");
		stopwords.add("the");
		stopwords.add("this");
		stopwords.add("to");
		stopwords.add("was");
		stopwords.add("what");
		stopwords.add("when");
		stopwords.add("where");
		stopwords.add("who");
		stopwords.add("will");
		stopwords.add("with");
		stopwords.add("and");

		stopwords.add("category");
		stopwords.add("file");
		stopwords.add("template");
		stopwords.add("help");
	}

	public Map<String, List<String>> normalize(String text) {

		int positionCounter = -1;
		Map<String, List<String>> result = new HashMap<String, List<String>>();
		// whitespace tokenization
		StringTokenizer st = new StringTokenizer(text);

		while (st.hasMoreTokens()) {
			String s = st.nextToken().toLowerCase();

			positionCounter++;

			// links are thrown away
			if (s.startsWith("http://"))
				continue;

			s = s.replace('-', ' ');
			s = s.replace('/', ' ');
			s = s.replace('|', ' ');
			s = s.replace('.', ' ');
			s = s.replace(':', ' ');
			s = s.replace("&quot;", "");
			s = s.replace("&amp;", "");
			s = s.replace("&lt;", "");
			s = s.replace("&gt;", "");

			// everything that is not alphanumeric is thrown away
			s = s.replaceAll("[^a-z]", "");

			// stopwords, empty strings and strings with 20+ characters are
			// thrown out
			if (stopwords.contains(s) || s.length() == 0 || s.length() > 20)
				continue;

			if (!result.containsKey(s)) {
				result.put(s, new ArrayList<String>());
			}

			result.get(s).add(String.valueOf(positionCounter));
		}

		return result;
	}

}
