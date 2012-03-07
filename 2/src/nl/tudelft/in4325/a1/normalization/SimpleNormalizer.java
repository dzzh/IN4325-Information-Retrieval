package nl.tudelft.in4325.a1.normalization;

import java.util.*;

/**
 * Simple implementation of {@link Normalizer} which does tokenization according to white spaces.
 */
public class SimpleNormalizer implements Normalizer{
    
    public Map<String, List<String>> normalize(String text){
        StringTokenizer st = new StringTokenizer(text);
        int positionCounter = 0;
        Map<String, List<String>> result = new HashMap<String, List<String>>();

        while(st.hasMoreTokens())
        {
            String word = st.nextToken();
            if (!result.containsKey(word)) {
                result.put(word, new ArrayList<String>());
            }

            result.get(word).add(String.valueOf(positionCounter));

            positionCounter++;
        }

        return result;
    }
}
