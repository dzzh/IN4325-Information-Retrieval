package nl.tudelft.in4325.a1.normalization;

import info.bliki.wiki.filter.PlainTextConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;

import nl.tudelft.in4325.a1.indexing.TextArrayWritable;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Normalizes corpus of XML documents with advanced tokenization algorithm.
 */
public class AdvancedNormalizationMapper extends SimpleNormalizationMapper{

	private static final Logger LOGGER = LoggerFactory.getLogger(AdvancedNormalizationMapper.class);
	
	//used to extract document id and contents from Wikipedia XML
	private static final String ID_TAG = "id";
	private static final String TEXT_TAG = "text";

    private Text word = new Text();
    
    private AdvancedNormalizer normalizer = new AdvancedNormalizer();
    
    /**
     * Advanced mapper to parse Wikipedia XMLs. 
     * Removes wiki markup and HTML formatting, tokenizes the text and applies Porter2 stemming algorithm to tokens.
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
    	
    	String stringValue = value.toString();
    	
    	//parsing the document contents
    	int id = Integer.valueOf(extractContents(stringValue, ID_TAG)).intValue();
    	String text = extractContents(stringValue, TEXT_TAG);
    	if (text.length() == 0){
    		LOGGER.warn("No text was extracted for id " + id);
    	}
    	
    	text = text.toLowerCase(Locale.ENGLISH);
    	
    	//removing Wikipedia formatting
        String plainText = normalizer.getWikiModel().render(new PlainTextConverter(), text);
        
        //removing HTML tags
        plainText = normalizer.removeHtmlTags(plainText);
                
        //additional markup removals
        plainText = normalizer.removeMarkup(plainText);
        
        StringTokenizer st = new StringTokenizer(plainText, " \t\n\r\f\\/");
        int positionCounter = 0;
		Map<String, List<String>> wordPossitions = new HashMap<String, List<String>>();
		
        while(st.hasMoreTokens())
        {
        	String token = st.nextToken();
        	
            //removing noise symbols (leaving alpha plus couple of others)	 
            token = normalizer.removeNonAlphaNumericSymbols(token);
            
        	if (normalizer.isTokenValid(token)){
        		//Applying Porter2 stemming algorithm and emit results
        		normalizer.getStemmer().setCurrent(token);
        		normalizer.getStemmer().stem();
		        String word = normalizer.getStemmer().getCurrent();
				if (!wordPossitions.containsKey(word)) {
					wordPossitions.put(word, new ArrayList<String>());
				}

				wordPossitions.get(word).add(String.valueOf(positionCounter));
	        }
        	
        	positionCounter++;
        }
        
        for (String term : wordPossitions.keySet()) {
			Writable[] tuples = {
					new Text(String.valueOf(id)),
					new Text("< " + StringUtils.join(wordPossitions.get(term).toArray(new String[0]), ",") + " >") };
			TextArrayWritable writableArrayWritable = new TextArrayWritable();
			writableArrayWritable.set(tuples);
			word.set(term);
			context.write(word, writableArrayWritable);
		}
        
    }
    
}
