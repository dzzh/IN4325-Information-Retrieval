package nl.tudelft.in4325.a1.normalization;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Normalizes corpus of XML documents with tokenization according to white spaces.
 */
public class SimpleNormalizationMapper extends Mapper<Object, Text, Text, IntWritable>{
	
	private static final String ID_TAG = "id";
	private static final String TEXT_TAG = "text";
	
    private Text word = new Text();

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleNormalizationMapper.class);
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
    	//parsing the document contents
    	String stringValue = value.toString();
    	
    	int id = Integer.valueOf(extractContents(stringValue, ID_TAG)).intValue();
    	String text = extractContents(stringValue, TEXT_TAG);
    	if (text.length() == 0){
    		LOGGER.warn("No text was extracted for id " + id);
    	}
    	
    	//normalization and output
        StringTokenizer st = new StringTokenizer(text);
        while(st.hasMoreTokens())
        {
            word.set(st.nextToken());
            context.write(word, new IntWritable(id));
        }
    }
    
    /**
     * Extract contents of a specified tag from submitted XML data. Only first occurrence 
     * for data with existent starting and ending tags is considered. 
     * @param value XML data
     * @param tag tag to search with no brackets
     * @return contents inside the tag
     */
    protected String extractContents(String value, String tag){
    	if(value.length() < 100){
    		LOGGER.info(value);
    	}
    	int start = value.indexOf("<" + tag);
    	
    	if (start > -1){
    		//find matching closing bracket and move start pointer behind it
    		start = value.indexOf(">", start) + 1;
    	}
    	
    	int end = value.indexOf("</" + tag + ">");
    	if (start > -1 && end > -1){
    		return value.substring(start, end);
    	} else {
    		LOGGER.warn("Cannot extract contents: tag not found");
    		return "";
    	}
    }
    
}
