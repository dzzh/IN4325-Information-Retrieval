package nl.tudelft.in4325.a2.tfidf;

import java.util.Map;

import nl.tudelft.in4325.Constants;
import nl.tudelft.in4325.a2.utils.QueryParser;

public class AdvancedTFIDFMapper extends SimpleTFIDFMapper {

	@Override
	protected Map<String, Map<String, Integer>> extractQueries() {
		//The query terms are pre-processed (normalized)
		return new QueryParser(true).parserQuery(Constants.QUERIES_FILE);
	}
}
