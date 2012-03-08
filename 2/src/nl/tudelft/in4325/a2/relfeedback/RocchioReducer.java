package nl.tudelft.in4325.a2.relfeedback;

import nl.tudelft.in4325.ConfigurationHelper;
import nl.tudelft.in4325.Constants;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class RocchioReducer extends Reducer<Text,Text,Text,Text> {

    private int termsToadd;
    
	public RocchioReducer(){
        Configuration appConfig = new ConfigurationHelper().getConfiguration();
        termsToadd = appConfig.getInt("rocchio-add-terms");
	}
	
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
        List<TermScore> scores = parseQueryScores(values);
        Collections.sort(scores, new TermScoreComparatorByScore());
        int modifiedQuerySize = getOriginalQuerySize(key.toString()) + termsToadd;
        if (scores.size() > modifiedQuerySize){
            scores = scores.subList(0, modifiedQuerySize);
        }
        String modifiedQuery = "";
        
        for (TermScore ts : scores){
            modifiedQuery += ts.getTerm() + "[" + ts.getScore() + "]" + " ";
        }
        modifiedQuery = modifiedQuery.trim();
        
        context.write(new Text(getQueryId(key.toString())), new Text(modifiedQuery));
    }
    
    private String getQueryId(String key){
        return key.split(Constants.FIELD_SEPARATOR)[0];
    }
    
    private int getOriginalQuerySize(String key){
        return Integer.valueOf(key.split(Constants.FIELD_SEPARATOR)[1]);
    }
    
    private List<TermScore> parseQueryScores(Iterable<Text> values){
        List<TermScore> result = new ArrayList<TermScore>();
        
        for (Text t : values){
            String[] data = t.toString().split(Constants.FIELD_SEPARATOR);
            result.add(new TermScore(data[0], Double.valueOf(data[1])));
        }
        
        return result;
    }
    
    private class TermScore{
        private String term;
        private double score;

        private TermScore(String term, double score) {
            this.term = term;
            this.score = score;
        }

        public String getTerm() {
            return term;
        }

        public double getScore() {
            return score;
        }

        @Override
        public String toString() {
            return term + "[" + score + "]";
        }
    }
    
    public static class TermScoreComparatorByScore implements Comparator<TermScore>{

        @Override
        public int compare(TermScore o1, TermScore o2) {
            return Double.compare(o2.getScore(), o1.getScore());
        }

    }
}
