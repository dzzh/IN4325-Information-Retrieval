package nl.tudelft.in4325.a2.relfeedback;

import nl.tudelft.in4325.ConfigurationHelper;
import nl.tudelft.in4325.a1.normalization.NormalizationType;
import nl.tudelft.in4325.a2.utils.QueryParser;
import nl.tudelft.in4325.a2.utils.WordIndexExtractor;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RocchioMapper extends Mapper<Object, Text, Text, Text> {

    private static final Log LOGGER = LogFactory.getLog(RocchioMapper.class);
    
    private final static double ALPHA = 1;
    private final static double BETA = 0.75;
    private final static double GAMMA = 0;

    private final int numberOfDocuments;
    private final Map<String, Map<String, Integer>> queries;
    private final WordIndexExtractor wordIndexExtractor = new WordIndexExtractor();
    
	private final List<QueryRelevance> queryRelevances = new QrelsProcessor().getQueriesRelevance();
    
    public RocchioMapper() {
        Configuration appConfig = new ConfigurationHelper().getConfiguration();
        String platform = appConfig.getString("target-platform");
        numberOfDocuments = Integer.valueOf(appConfig.getString(platform + "-number-of-documents"));
        String type = appConfig.getString("normalization-type");
        NormalizationType normalizationType = NormalizationType.getNormalizationType(type);
        String queriesFile = appConfig.getString(platform + "-queries-file");
        queries = new QueryParser(normalizationType.getNormalizer()).parseQueries(queriesFile);
        queryRelevances.size();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        // parsing the document contents
        String stringValue = value.toString();
        String word = stringValue.substring(0, stringValue.indexOf("\t")).trim();
        Map<Integer, Integer> docNumberOfOccurrences = wordIndexExtractor.extractWordFrequencies(stringValue);

        for (String query : queries.keySet()) {
            
            QueryRelevance relevance = null;
            
            for (QueryRelevance qr : queryRelevances){
                if (query.equals(qr)){
                    relevance = qr;
                    break;
                }
            }
            
            if (relevance == null){
                LOGGER.warn("Relevance data not found for query " + query);
                return;
            }
            
            if (isTermSignificant((String)key, docNumberOfOccurrences, queries.get(query), relevance)) {

                //Compute Rocchio score for the term and emit it

                // calculate the IDF of the word
                double wordIDF = Math.log(numberOfDocuments / docNumberOfOccurrences.size());

                // calculate the TF.IDF of the query term
                double queryTFIDF = queries.get(query).get(word) * wordIDF;

                for (Integer docId : docNumberOfOccurrences.keySet()) {

                    // calculate the TF.IDF of the document term
                    //double documentTFIDF = calculateTFIDF(docNumberOfOccurrences.get(docId), wordIDF);

//                    if (documentTFIDF != 0) {
//                        context.write(new Text(query), new Text(word + ","
//                                + docId + "," + documentTFIDF + "," + queryTFIDF));
//                    }
                }
            }
        }
    }

    private boolean isTermSignificant(String term, Map<Integer, Integer> termDocs,
                                      Map<String, Integer> query, QueryRelevance relevance){
        for (String queryTerm : query.keySet()){
            if (queryTerm.equals(term)){
                return true; 
            }
        }
        
        for (int termDoc : termDocs.keySet()){
            for (int relDoc : relevance.getRelevantDocuments()){
                if (termDoc == relDoc){
                    return true;
                }
            }

            for (int irrelDoc : relevance.getIrrelevantDocuments()){
                if (termDoc == irrelDoc){
                    return true;
                }
            }
        }

        return false;
    }

}
