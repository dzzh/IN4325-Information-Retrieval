package nl.tudelft.in4325.a2.relfeedback;

import java.util.Comparator;

/**
 * A simple data class to keep document ID to its TF.IDF weight correspondence
 */
public class DocScore {
    
    private int docId;
    private double score;

    public DocScore(int docId, double score) {
        this.docId = docId;
        this.score = score;
    }

    public int getDocId() {
        return docId;
    }

    public double getScore() {
        return score;
    }
    
    public static class DocScoreComparatorByScore implements Comparator<DocScore>{

        @Override
        public int compare(DocScore o1, DocScore o2) {
            return Double.compare(o2.getScore(), o1.getScore());
        }
    }
}
