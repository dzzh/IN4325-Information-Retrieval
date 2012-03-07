package nl.tudelft.in4325.a2.relfeedback;

import nl.tudelft.in4325.ConfigurationHelper;
import nl.tudelft.in4325.a1.normalization.NormalizationType;
import nl.tudelft.in4325.a2.utils.QueryParser;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RocchioMapper extends Mapper<Object, Text, Text, Text> {

    private final int numberOfDocuments;
    private final Map<String, Map<String, Integer>> queries;
    private final List<QueryRelevance> queryRelevances = new QrelsProcessor().getQueriesRelevance();
    
    public RocchioMapper() {
        Configuration appConfig = new ConfigurationHelper().getConfiguration();
        String platform = appConfig.getString("target-platform");
        numberOfDocuments = Integer.valueOf(appConfig.getString(platform + "-number-of-documents"));
        String type = appConfig.getString("normalization-type");
        NormalizationType normalizationType = NormalizationType.getNormalizationType(type);
        String queriesFile = appConfig.getString(platform + "-queries-file");
        queries = new QueryParser(normalizationType.getNormalizer()).parseQuery(queriesFile);
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

    }

}
