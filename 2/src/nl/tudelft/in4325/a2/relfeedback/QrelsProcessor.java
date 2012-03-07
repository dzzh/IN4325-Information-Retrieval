package nl.tudelft.in4325.a2.relfeedback;

import nl.tudelft.in4325.ConfigurationHelper;
import nl.tudelft.in4325.Constants;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOError;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class QrelsProcessor {
    
    private static final Log LOGGER = LogFactory.getLog(QrelsProcessor.class);
    
    private Map<String, List<Integer>> relevance;
    private Map<String, List<Integer>> firstRunResults;

    FileSystem fs = null;
    
    public QrelsProcessor(){
        Configuration appConfig = new ConfigurationHelper().getConfiguration();
        String platform = appConfig.getString("target-platform");
        String qrelsFile = appConfig.getString(platform + "-qrels-file");
        String firstRunFile = appConfig.getString(platform + "-first-run-file");
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
            List<Integer> firstRunDocIds = firstRunResults.get(queryId);
            List<Integer> relvanceDocIds = relevance.get(queryId);
            QueryRelevance qr = new QueryRelevance(queryId);
            queryRelevances.add(qr);
            for (int docId : firstRunDocIds){
                if (relvanceDocIds.contains(docId)){
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
    
    private Map<String, List<Integer>> getFirstRunResults(String firstRunFile){
        Map<String, List<Integer>> result = new HashMap<String, List<Integer>>();
        Path inFile = new Path(firstRunFile);
        try{
            if (!fs.exists(inFile)){
                LOGGER.error("Input file with first run results does not exist");
                return null;
            }

            FSDataInputStream in = fs.open(inFile);
            String strLine = null;
            while ((strLine = in.readLine()) != null){
                String[] data = strLine.split("\t");
                if (result.get(data[0]) == null){
                    result.put(data[0], new LinkedList<Integer>());
                }
                result.get(data[0]).add(Integer.valueOf(data[1].split(" ")[0]));
            }
            
            return result;

        } catch (IOException e){
            LOGGER.error("I/O exception while working with first run file");
            return null;
        }
    }
}
