package nl.tudelft.mapred.wikipedia;

import java.io.IOException;
import java.lang.InterruptedException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CategoryCountMapper extends Mapper<Object, Text, Text, IntWritable>
{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        StringTokenizer st = new StringTokenizer(value.toString());
        while(st.hasMoreTokens())
        {
            word.set(st.nextToken());
            context.write(word,one);
        }
    }
}