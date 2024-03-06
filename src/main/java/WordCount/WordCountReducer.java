package WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException, IOException {
        //汇总单词次数
        int sum = 0;
        for (IntWritable v : values) {
            sum += v.get();
        }
        //输出
        context.write(key, new IntWritable(sum));
    }
}
