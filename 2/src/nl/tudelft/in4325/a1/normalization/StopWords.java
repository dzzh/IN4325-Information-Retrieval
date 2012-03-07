package nl.tudelft.in4325.a1.normalization;

import nl.tudelft.in4325.Constants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.List;

public class StopWords {
    
    private static final Log LOGGER = LogFactory.getLog(StopWords.class);
    
    private static List<String> stopWords = null;

    private StopWords(){}

    /**
     * Provides with corpus-dependent list of stop words.
     * Stop words are read from file once, then the internal representation is used.
     * @return stop words
     */
    public static List<String> getStopWords(){
        if (stopWords == null){
            readStopWords();
        }
        
        return stopWords;
    }
    
    /**
     * Reads corpus-dependent stop words from a file (one word at one line) and saves them to list
     * @return list of stop words
     */
    private static List<String> readStopWords() {
        stopWords = new LinkedList<String>();

        try {
            FileReader fr = new FileReader(Constants.STOPWORDS_FILE);
            BufferedReader in = new BufferedReader(fr);
            String strLine;
            while ((strLine = in.readLine()) != null) {
                if (!strLine.startsWith("#") && !strLine.isEmpty()) {
                    stopWords.add(strLine);
                }
            }
            in.close();
            fr.close();
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
            System.exit(1);
        }

        return stopWords;
    }
}
