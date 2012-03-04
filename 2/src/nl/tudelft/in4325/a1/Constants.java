package nl.tudelft.in4325.a1;

/**
 * Container for static constants used throughout the code. 
 */
public final class Constants {
	public static final String PROPERTIES_FILE = "conf/conf.ini";
	public static final String DEFAULT_PROPERTIES_FILE = "conf/conf.default.ini";
	public static final String STOPWORDS_FILE = "conf/stopwords.txt";
	
	public static final String FIELD_SEPARATOR = ":";
	
	private Constants(){}
	
	/**
	 *	MapReduce jobs names
	 */
	public static enum Jobs{
		SIMPLE_NORMALIZATION,
		ADVANCED_NORMALIZATION,
		NORMALIZATION_REPORT_AGGREGATION,
		NORMALIZATION_REPORT_FREQUENCY,
		INVERTED_INDEXING
	}

	/**
	 * Used to identify measurement parameters for normalization analysis 
	 */
	public static enum Metrics{
		UNIQUE_TERM,
		NON_UNIQUE_TERM,
		OCCURRING_ONCE
	}
}
