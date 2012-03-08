package nl.tudelft.in4325.a2.relfeedback;

import nl.tudelft.in4325.ConfigurationHelper;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class QrelsProcessor {
    
    private static final Log LOGGER = LogFactory.getLog(QrelsProcessor.class);
    
    private Map<String, List<Integer>> relevance;
    private Map<String, List<DocScore>> firstRunResults;
    private int topRetrievedDocs;

    FileSystem fs = null;
    
    public QrelsProcessor(){
        Configuration appConfig = new ConfigurationHelper().getConfiguration();
        String platform = appConfig.getString("target-platform");
        String qrelsFile = appConfig.getString(platform + "-qrels-file");
        String firstRunFile = appConfig.getString(platform + "-first-run-file");
        topRetrievedDocs = appConfig.getInt("rocchio-top-retrieved-docs");
        
        relevance = getRelevance(qrelsFile);

        try{
            fs = FileSystem.get(new org.apache.hadoop.conf.Configuration());
        }  catch (IOException e){
            LOGGER.error("I/O exception occurred while working with HDFS");
            System.exit(1);
        }
        
        firstRunResults = getFirstRunResults(firstRunFile);
    }

    public List<QueryRelevance> getQueriesRelevance(){
        List<QueryRelevance> queryRelevances = new LinkedList<QueryRelevance>();
        for (String queryId : firstRunResults.keySet()){
            List<DocScore> firstRunDocIds = firstRunResults.get(queryId);
            List<Integer> relevanceDocIds = relevance.get(queryId);
            QueryRelevance qr = new QueryRelevance(queryId);
            queryRelevances.add(qr);
            for (DocScore docScore : firstRunDocIds){
                int docId = docScore.getDocId();
                boolean found = false;
                for (Integer docScore1 : relevanceDocIds){
                    if (docId == docScore1){
                        found = true;
                        break;
                    }
                }
                if (found){
                    qr.addRelevantDocument(docId);
                } else {
                    qr.addIrrelevantDocument(docId);
                }
            }
        }
        return queryRelevances;
    }
    
    private Map<String, List<Integer>> getRelevance(String qrelsFile){
        Map<String, List<Integer>> result = new HashMap<String, List<Integer>>();
        try {
            FileReader fr = new FileReader(qrelsFile);
            BufferedReader in = new BufferedReader(fr);
            String strLine;
            while ((strLine = in.readLine()) != null) {
                String[] data = strLine.split(" ");
                if (result.get(data[0]) == null){
                    result.put(data[0], new LinkedList<Integer>());
                }
                result.get(data[0]).add(Integer.valueOf(data[2]));
            }
            in.close();
            fr.close();
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
            return null;
        }
        return result;
    }
    
    private Map<String, List<DocScore>> getFirstRunResults(String firstRunFile){
        Map<String, List<DocScore>> result = new HashMap<String, List<DocScore>>();
        Path inFile = new Path(firstRunFile);
        try{
            if (!fs.exists(inFile)){
                LOGGER.error("Input file with first run results does not exist");
                return null;
            }

            //reading data from file
            FSDataInputStream in = fs.open(inFile);
            String strLine;
            while ((strLine = in.readLine()) != null){
                String[] data = strLine.split("\t");
                if (result.get(data[0]) == null){
                    result.put(data[0], new LinkedList<DocScore>());
                }
                String[] docData = data[1].split(" ");
                int docId = Integer.valueOf(docData[0]);
                double score = Double.valueOf(docData[1]);
                result.get(data[0]).add(new DocScore(docId, score));
            }

            //sorting and leaving only N top results
            for (String queryId : result.keySet()){
                List<DocScore> docScores = result.get(queryId);
                Collections.sort(docScores, new DocScore.DocScoreComparatorByScore());
                if (docScores.size() > topRetrievedDocs){
                    result.put(queryId, docScores.subList(0, topRetrievedDocs));
                }
            }
            
            return result;

        } catch (IOException e){
            LOGGER.error("I/O exception while working with first run file");
            return null;
        }
    }

}
