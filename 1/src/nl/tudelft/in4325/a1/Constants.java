package nl.tudelft.in4325.a1;

public final class Constants {
	public static final String PROPERTIES_FILE = "conf/conf.ini";
	public static final String DEFAULT_PROPERTIES_FILE = "conf/conf.default.ini";
	
	public static final String FIELD_SEPARATOR = ":";
	
	private Constants(){}
	
	public static enum Jobs{
		SIMPLE_NORMALIZATION,
		COMPLEX_NORMALIZATION,
		NORMALIZATION_REPORT_AGGREGATION,
		NORMALIZATION_REPORT_FREQUENCY,
		INVERTED_INDEXING
	}

	public static enum Metrics{
		UNIQUE_TERM,
		NON_UNIQUE_TERM,
		OCCURRING_ONCE
	}
}
