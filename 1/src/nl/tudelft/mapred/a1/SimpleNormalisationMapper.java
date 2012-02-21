package nl.tudelft.mapred.a1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleNormalisationMapper extends Mapper<Object, Text, Text, Text>{
	
	private static final String ID_TAG = "id";
	private static final String TEXT_TAG = "text";
	
    private Text word = new Text();

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleNormalisationMapper.class);
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
    	//parsing the document contents
    	String stringValue = value.toString();
    	
    	int id = Integer.valueOf(extractContents(stringValue, ID_TAG)).intValue();
    	LOGGER.info(String.valueOf(id));
    	String text = extractContents(stringValue, TEXT_TAG);
    	if (text.length() == 0){
    		LOGGER.warn("No text was extracted for id " + id);
    	}
    	
    	Text documentId = new Text(String.valueOf(id));
    	
        StringTokenizer st = new StringTokenizer(text);
        while(st.hasMoreTokens())
        {
            word.set(st.nextToken());
            context.write(word,documentId);
        }
    }
    
    private String extractContents(String value, String tag){
    	if(value.length() < 100){
    		LOGGER.info(value);
    	}
    	int start = value.indexOf("<" + tag + ">") + tag.length() + 2;
    	int end = value.indexOf("</" + tag + ">");
    	if (start > -1 && end > -1){
    		return value.substring(start, end);
    	} else {
    		LOGGER.warn("Cannot extract contents: tag not found");
    		return "";
    	}
    }
    
}
