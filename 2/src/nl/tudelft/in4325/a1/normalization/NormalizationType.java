package nl.tudelft.in4325.a1.normalization;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Type of normalization. Simple just works with spaces tokenization, advanced uses more complex algorithms.
 */
public enum NormalizationType {
    SIMPLE(new SimpleNormalizer()),
    ADVANCED(new AdvancedNormalizer());

    private static final Log LOGGER = LogFactory.getLog(NormalizationType.class);

    NormalizationType(Normalizer normalizer) {
        this.normalizer = normalizer;
    }

    private Normalizer normalizer;

    public Normalizer getNormalizer() {
        return normalizer;
    }

    public static NormalizationType getNormalizationType(String type) {
        if (type.equalsIgnoreCase("simple")) {
            return NormalizationType.SIMPLE;
        } else if (type.equalsIgnoreCase("advanced")) {
            return NormalizationType.ADVANCED;
        } else {
            LOGGER.warn("Wrong normalization type specified, advanced one is selected");
            return NormalizationType.ADVANCED;
        }
    }
}