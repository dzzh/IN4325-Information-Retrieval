package nl.tudelft.mapred.wikipedia;

import java.io.IOException;
import java.lang.InterruptedException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CategoryCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        int sum = 0;
        for(IntWritable iw : values)
        {
            sum += iw.get();
        }
        context.write(key, new IntWritable(sum));
    }
}