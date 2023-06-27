package PreJob;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        solve(line, context);

    }
    public void solve(String str, Context context) throws IOException, InterruptedException {
        for(char ch : str.toCharArray()){
            context.write(new Text(Character.toString(ch)), new IntWritable(1));
        }

    }
}
