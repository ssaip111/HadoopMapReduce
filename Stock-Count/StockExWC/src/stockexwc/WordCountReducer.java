package stockexwc;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer 
        extends Reducer<Text, IntWritable, Text, IntWritable>{

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, 
            Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        Iterator<IntWritable> itr = values.iterator();
        while (itr.hasNext()) {
            sum += itr.next().get();
        }

        context.write(key, new IntWritable(sum));
    }
}