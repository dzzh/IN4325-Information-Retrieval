package nl.tudelft.in4325.a1.normalization;

import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;
import org.jsoup.Jsoup;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.englishStemmer;

import java.util.*;

/**
 * Advanced implementation of {@link Normalizer}.
 * Removes wiki markup and HTML formatting, tokenizes the text and applies Porter2 stemming algorithm to tokens.
 */
public class AdvancedNormalizer implements Normalizer{

	// Third-party library to remove Wikipedia formatting
	private WikiModel wikiModel = new WikiModel(
			"http://www.mywiki.com/wiki/${image}",
			"http://www.mywiki.com/wiki/${title}");

	// Porter2 stemmer for English language
	private SnowballStemmer stemmer = new englishStemmer();

	// Symbols allowed in tokens, apart from letters, digits and white spaces
	private static final String allowedSymbols = "%-_";

	String curlyBraces = "{}";
	char leftBrace = curlyBraces.charAt(0);
	char rightBrace = curlyBraces.charAt(1);

    public Map<String, List<String>> normalize(String text){
        text = text.toLowerCase(Locale.ENGLISH);

        //removing Wikipedia formatting
        String plainText = getWikiModel().render(new PlainTextConverter(), text);

        //removing HTML tags
        plainText = removeHtmlTags(plainText);

        //additional markup removals
        plainText = removeMarkup(plainText);

        StringTokenizer st = new StringTokenizer(plainText, " \t\n\r\f\\/");
        int positionCounter = 0;
        Map<String, List<String>> result = new HashMap<String, List<String>>();

        while(st.hasMoreTokens())
        {
            String token = st.nextToken();

            //removing noise symbols (leaving alpha plus couple of others)	 
            token = removeNonAlphaNumericSymbols(token);

            if (isTokenValid(token)){
                //Applying Porter2 stemming algorithm and emit results
                stemmer.setCurrent(token);
                stemmer.stem();
                String word = stemmer.getCurrent();
                if (!result.containsKey(word)) {
                    result.put(word, new ArrayList<String>());
                }

                result.get(word).add(String.valueOf(positionCounter));
            }

            positionCounter++;
        }
        
        return result;
    }
    
	/**
	 * Removes noise from tokens
	 * 
	 * @param text token to process
	 * @return result
	 */
	private String removeNonAlphaNumericSymbols(String text) {

		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < text.length(); i++) {
			char c = text.charAt(i);
			if (Character.isLetterOrDigit(c) || Character.isWhitespace(c)
					|| allowedSymbols.indexOf(c) > -1) {
				sb.append(c);
			}
		}

		return sb.toString();
	}

	/**
	 * Determines validity of a token by comparison with a list of stop words
	 * 
	 * @param token current token
	 * @return true if the token is valid, false otherwise
	 */
	private boolean isTokenValid(String token) {

		if (token.length() == 0) {
			return false;
		}

        List<String> stopWords = StopWords.getStopWords();
        
		for (String stopWord : stopWords) {
			if (token.startsWith(stopWord)) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Removes HTML formatting from text
	 * 
	 * @param html HTML-formatted String
	 * @return plain text String
	 */
	private String removeHtmlTags(String html) {
		return Jsoup.parse(html).text();
	}

	/**
	 * Additional wiki markup removal procedures
	 * 
	 * @param text text with markup
	 * @return plain text
	 */
	private String removeMarkup(String text) {

		StringBuilder sb = new StringBuilder();

        //adds first symbol for correct further processing
        char first = text.charAt(0);
        if (first != leftBrace && first != rightBrace){
            sb.append(first);
        }

		int braces = 0;

		// removes any content between two or more consequent pairs of curly
		// braces and the braces themselves
		for (int i = 1; i < text.length(); i++) {
			char current = text.charAt(i);
			char previous = text.charAt(i - 1);
			if (current == leftBrace && previous == leftBrace) {
				braces++;
			} else if (current == rightBrace && previous == rightBrace) {
				braces--;
			}
			if (braces == 0 && curlyBraces.indexOf(current) == -1) {
				sb.append(current);
			}
		}

		return sb.toString();
	}

	private WikiModel getWikiModel() {
		return wikiModel;
	}
}
