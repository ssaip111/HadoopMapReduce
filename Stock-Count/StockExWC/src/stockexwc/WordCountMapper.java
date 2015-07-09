package stockexwc;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper 
        extends Mapper<LongWritable, Text, Text, IntWritable>{
    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    	String[] split = value.toString().split("\t+");
        word.set(split[1]);
        if (split.length > 2) {
            try {
              context.write(word, one);
            } catch (NumberFormatException e) {
              // cannot parse - ignore
            }
          }
    }
}