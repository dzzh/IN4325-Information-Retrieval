package nl.tudelft.in4325.a2.relfeedback;

import nl.tudelft.in4325.ConfigurationHelper;
import nl.tudelft.in4325.Constants;
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

    private final static double ALPHA = 100;
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
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        // parsing the document contents
        String stringValue = value.toString();
        String word = stringValue.substring(0, stringValue.indexOf("\t")).trim();
        Map<Integer, Integer> docNumberOfOccurrences = wordIndexExtractor.extractWordFrequencies(stringValue);

        for (String query : queries.keySet()) {
            
            QueryRelevance relevance = null;
            
            for (QueryRelevance qr : queryRelevances){
                if (query.equals(qr.getQueryId())){
                    relevance = qr;
                    break;
                }
            }
            
            if (relevance == null){
                LOGGER.warn("Relevance data not found for query " + query);
                return;
            }
            
            computeRocchioScore(word, query, docNumberOfOccurrences, queries.get(query), relevance, context);
        }
    }

    private boolean computeRocchioScore(String term, String queryId, Map<Integer, Integer> termDocs,
                                    Map<String, Integer> query, QueryRelevance relevance,
                                    Context context) throws IOException, InterruptedException{
        
        double score = 0;

        double termIDF = Math.log(numberOfDocuments / termDocs.size());
        double queryScore = 0;
        double positiveDocScore = 0;
        double negativeDocScore = 0;

        for (String queryTerm : query.keySet()){
            if (queryTerm.equals(term)){
                queryScore = ALPHA * query.get(term) * termIDF;
                break;
            }
        }
        
        for (int termDoc : termDocs.keySet()){
            for (int relDoc : relevance.getRelevantDocuments()){
                if (termDoc == relDoc){
                    positiveDocScore += termIDF * termDocs.get(relDoc);
                }
            }
        }
        positiveDocScore *= BETA;

        for (int termDoc : termDocs.keySet()){
            for (int irrelDoc : relevance.getIrrelevantDocuments()){
                if (termDoc == irrelDoc){
                    negativeDocScore += termIDF * termDocs.get(irrelDoc);
                }
            }
        }
        negativeDocScore *= -1 * GAMMA;

        score = queryScore + positiveDocScore + negativeDocScore;

        if (score > 0){
            int querySize = queries.get(queryId).size();
            context.write(new Text(queryId + Constants.FIELD_SEPARATOR + querySize),
                          new Text(term + Constants.FIELD_SEPARATOR + score));
        }

        return false;
    }

}
