package nl.tudelft.in4325.a2.relfeedback;

import java.util.Comparator;

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
            return Double.compare(o1.getScore(), o2.getScore());
        }
    }
}
