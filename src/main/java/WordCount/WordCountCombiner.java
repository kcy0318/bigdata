package WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //归并过程：相当于小计，相同的键，进行小计
        int sum = 0;
        for (IntWritable v : values) {
            sum += v.get();
        }
        //输出
        System.out.println("小计:(" + key.toString() + ", " + sum + ")");
        context.write(key , new IntWritable(sum));
    }
}
