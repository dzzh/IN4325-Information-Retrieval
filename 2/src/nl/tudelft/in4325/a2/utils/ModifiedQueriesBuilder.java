package nl.tudelft.in4325.a2.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

/**
 * Generates XML data from Hadoop output for modified queries generated in Rocchio algorithm
 */
public class ModifiedQueriesBuilder {
    
    public static void main(String[] args){
        try {
            FileReader fr = new FileReader(args[0]);
            BufferedReader in = new BufferedReader(fr);
            FileWriter fw = new FileWriter(args[1]);
            BufferedWriter out = new BufferedWriter(fw);
            String strLine;
            while ((strLine = in.readLine()) != null) {
                String[] data = strLine.split("\t");
                String queryId = data[0];
                String modifiedQuery = data[1];
                out.write("<DOC>\n");
                out.write("<DOCNO>"+queryId+"</DOCNO>\n");
                out.write("<TEXT>\n" + modifiedQuery + "\n</TEXT>\n");
                out.write("</DOC>\n\n");
            }
            in.close();
            fr.close();
            out.close();
            fw.close();
        } catch (Exception e) {
            System.exit(1);
        }
        
    }
}
