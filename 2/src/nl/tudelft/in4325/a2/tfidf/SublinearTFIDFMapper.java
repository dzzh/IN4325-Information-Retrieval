package nl.tudelft.in4325.a2.tfidf;

/**
 * Implements the term weight normalization approach called "Sublinear tf scaling".
 *
 */
public class SublinearTFIDFMapper extends TFIDFMapper {

	@Override
	protected double calculateTFIDF(int frequency, double wordIDF) {
		if (frequency > 0) {
			return (1 + Math.log(frequency)) * wordIDF;
		} else {
			return 0;
		}
	}
}
