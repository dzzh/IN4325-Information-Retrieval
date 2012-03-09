package nl.tudelft.in4325.a2.relfeedback;

import java.util.LinkedList;
import java.util.List;

/**
 * Data class to keep references to relevant and irrelevant documents for certain queries
 */
public class QueryRelevance {
    
    private String queryId;
    private List<Integer> relevantDocuments = new LinkedList<Integer>();
    private List<Integer> irrelevantDocuments = new LinkedList<Integer>();
    
    public QueryRelevance(String queryId){
        this.queryId = queryId;
    }

    public String getQueryId() {
        return queryId;
    }

    public List<Integer> getRelevantDocuments() {
        return relevantDocuments;
    }

    public List<Integer> getIrrelevantDocuments() {
        return irrelevantDocuments;
    }
    
    public void addRelevantDocument(int docId){
        this.relevantDocuments.add(docId);
    }

    public void addIrrelevantDocument(int docId){
        this.irrelevantDocuments.add(docId);
    }
}
