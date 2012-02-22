package nl.tudelft.in4325.a1.normalization;

import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;

import nl.tudelft.in4325.a1.Constants;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.englishStemmer;

/**
 * Normalizes corpus of XML documents with advanced tokenization algorithm.
 */
public class AdvancedNormalizationMapper extends SimpleNormalizationMapper{

	private static final Logger LOGGER = LoggerFactory.getLogger(AdvancedNormalizationMapper.class);
	
	private static final String ID_TAG = "id";
	private static final String TEXT_TAG = "text";

    private Text word = new Text();
    
    //Class to help with removing Wiki formating
    private WikiModel wikiModel = new WikiModel("http://www.mywiki.com/wiki/${image}", "http://www.mywiki.com/wiki/${title}");
    
    //List of stopwords not to process
    private List<String> stopWords = readStopWords();
    
    //Porter2 stemmer for English
    private SnowballStemmer stemmer = new englishStemmer();
    
    private String allowedSymbols = "%-_";
    String curlyBraces = "{}";
	char leftBrace = curlyBraces.charAt(0);
	char rightBrace = curlyBraces.charAt(1);
    
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
    	
    	//normalization and output
    	
    	//removing Wikipedia formatting
        String plainText = wikiModel.render(new PlainTextConverter(), text);
        
        //removing HTML tags
        plainText = removeHtmlTags(plainText);
                
        //additional markup removals
        plainText = removeMarkup(plainText);
        
        StringTokenizer st = new StringTokenizer(plainText, " \t\n\r\f\\/");
        while(st.hasMoreTokens())
        {
        	String token = st.nextToken();
        	
            //removing noise symbols (leaving alpha plus couple of others)	 
            token = removeNonAlphaNumericSymbols(token);
        	if (isTokenValid(token)){
        		
        		//Applying Porter2 stemming algorithm and emit results
        		stemmer.setCurrent(token);
        		stemmer.stem();
		        word.set(stemmer.getCurrent());
		        context.write(word, new IntWritable(id));
	        }
        }
    }
    
    /**
     * Removes noise from tokens
     * @param text token to process
     * @return result
     */
    private String removeNonAlphaNumericSymbols(String text){
    	
    	StringBuffer sb = new StringBuffer();

    	for (int i = 0; i < text.length(); i++){
    		char c = text.charAt(i);
    		if (Character.isLetterOrDigit(c) || Character.isWhitespace(c) || allowedSymbols.indexOf(c) > -1){
    			sb.append(c);
    		}
    	}
    	
    	return sb.toString();
    }
    
    
    /**
     * Determines validity of a token by comparison with a list of stop words
     * @param token current token
     * @return true if the token is valid, false otherwise
     */
    private boolean isTokenValid(String token){
    	
    	if (token.length() == 0){
    		return false;
    	}
    	
    	for (String stopWord : stopWords){
    		if (token.startsWith(stopWord)){
    			return false;
    		}
    	}
    	
    	return true;	
    }
    
    /**
     * Reads corpus-dependent stop words from a file (one word at one line) and saves them to list  
     * @return list of stop words
     */
    private List<String> readStopWords(){
    	stopWords = new LinkedList<String>();
    	
    	try{
    		FileReader fr = new FileReader(Constants.STOPWORDS_FILE);
    		BufferedReader in = new BufferedReader(fr);
    		String strLine;
    		while ((strLine = in.readLine()) != null){
    			if (!strLine.startsWith("#") && !strLine.isEmpty()){
    				stopWords.add(strLine);
    			}
			}
    		in.close();
    		fr.close();
    	} catch (Exception e){
    		LOGGER.error("Error: ", e);
    		System.exit(1);
    	}
    	
    	return stopWords;
    }
    
    /**
     * Removes HTML formatting from text
     * @param html HTML-formatted String
     * @return plain text String
     */
    private static String removeHtmlTags(String html) {
        return Jsoup.parse(html).text();
    }
    
	/**
	 * Additional wiki markup removal procedures
	 * @param text text with markup
	 * @return plain text
	 */
    private String removeMarkup(String text){
    	
    	StringBuffer sb = new StringBuffer();
    	int braces = 0;
    	
    	for (int i = 1; i < text.length(); i++){
    		char current = text.charAt(i);
    		char previous = text.charAt(i-1);
    		if (current == leftBrace && previous == leftBrace){
    			braces++;
    		} else if (current == rightBrace && previous == rightBrace){
    			braces--;
    		}
    		if (braces == 0 && curlyBraces.indexOf(current) == -1){
    			sb.append(current);
    		}
    	}
    	
    	return sb.toString();
    }
}
