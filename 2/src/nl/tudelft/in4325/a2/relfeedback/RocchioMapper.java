package nl.tudelft.in4325.a2.relfeedback;

import nl.tudelft.in4325.ConfigurationHelper;
import nl.tudelft.in4325.Constants;
import nl.tudelft.in4325.a1.normalization.NormalizationType;
import nl.tudelft.in4325.a2.utils.QueryParser;
import nl.tudelft.in4325.a2.utils.WordIndexExtractor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Hadoop mapper to modify user queries based on Rocchio relevance feedback algorithm
 */
public class RocchioMapper extends Mapper<Object, Text, Text, Text> {

    private static final Log LOGGER = LogFactory.getLog(RocchioMapper.class);

    //setting alpha to large value keeps all the terms from the original query
    private final static double ALPHA = 100;

    private final static double BETA = 0.75;

    //values from the irrelevant documents do not affect the performance
    private final static double GAMMA = 0;

    private final int numberOfDocuments;
    private final Map<String, Map<String, Integer>> queries;
    private final WordIndexExtractor wordIndexExtractor = new WordIndexExtractor();
    
	private final List<QueryRelevance> queryRelevances = new QrelsProcessor().getQueriesRelevance();
    
    public RocchioMapper() {
        ConfigurationHelper appConfig = new ConfigurationHelper();
        numberOfDocuments = Integer.valueOf(appConfig.getPlatformDependentString("number-of-documents"));
        String type = appConfig.getString("normalization-type");
        NormalizationType normalizationType = NormalizationType.getNormalizationType(type);
        String queriesFile = appConfig.getPlatformDependentString("queries-file");
        queries = new QueryParser(normalizationType.getNormalizer()).parseQueries(queriesFile);
    }

    /**
     * Accepts the terms from word-level inverted index and computes Rocchio scores for them per query
     * @param key technical value, not used
     * @param value term from inverted index with references to documents and positions
     * @param context Hadoop context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

        //parses input value
        String stringValue = value.toString();
        String word = stringValue.substring(0, stringValue.indexOf("\t")).trim();
        Map<Integer, Integer> docNumberOfOccurrences = wordIndexExtractor.extractWordFrequencies(stringValue);

        //Data is generated in parallel for all the queries in the submitted file
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

    /**
     * Computes Rocchio score for given term and query
     * @param term term from the word-level inverted index
     * @param queryId id of the current query
     * @param termDocs documents containing the term and the frequencies of its occurrence
     * @param query text of query
     * @param relevance sets of relevant and irrelevant documents for query built in advance
     * @param context Hadoop context
     * @throws IOException
     * @throws InterruptedException
     */
    private void computeRocchioScore(String term, String queryId, Map<Integer, Integer> termDocs,
                                    Map<String, Integer> query, QueryRelevance relevance,
                                    Context context) throws IOException, InterruptedException{
        
        double termIDF = Math.log(numberOfDocuments / termDocs.size());
        double queryScore = 0;
        double positiveDocScore = 0;
        double negativeDocScore = 0;

        //Alpha-score if the term is presented in query
        for (String queryTerm : query.keySet()){
            if (queryTerm.equals(term)){
                queryScore = ALPHA * query.get(term) * termIDF;
                break;
            }
        }

        //Beta-score if term is presented in relevant documents
        for (int termDoc : termDocs.keySet()){
            for (int relDoc : relevance.getRelevantDocuments()){
                if (termDoc == relDoc){
                    positiveDocScore += termIDF * termDocs.get(relDoc);
                }
            }
        }
        positiveDocScore *= BETA;

        //Gamma-score if term is presented in irrelevant documents
        for (int termDoc : termDocs.keySet()){
            for (int irrelDoc : relevance.getIrrelevantDocuments()){
                if (termDoc == irrelDoc){
                    negativeDocScore += termIDF * termDocs.get(irrelDoc);
                }
            }
        }
        negativeDocScore *= -1 * GAMMA;

        //Computing resulting score
        double score = queryScore + positiveDocScore + negativeDocScore;

        //Negative and zero scores are not considered
        if (score > 0){
            int querySize = queries.get(queryId).size();
            context.write(new Text(queryId + Constants.FIELD_SEPARATOR + querySize),
                          new Text(term + Constants.FIELD_SEPARATOR + score));
        }
    }

}
