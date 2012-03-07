package nl.tudelft.in4325.a1.normalization;

import java.util.List;
import java.util.Map;

public interface Normalizer {

    /**
     * Normalizes provided String according to specific algorithm
     * @param text text to normalize
     * @return mapping from normalized terms to their frequency
     */
    public Map<String, List<String>> normalize(String text);
}
