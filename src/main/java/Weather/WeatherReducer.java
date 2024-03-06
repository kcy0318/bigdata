package Weather;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeatherReducer extends Reducer<WeatherWritable, Text, WeatherWritable, Text> {
    @Override
    protected void reduce(WeatherWritable key, Iterable<Text> values, Reducer<WeatherWritable, Text, WeatherWritable, Text>.Context context) throws IOException, InterruptedException {
        // 接收到的数据，按年份分组，分组按年份升序排列，分组内的数据按温度降序排列
        for (Text v : values) {
            // 输出
            context.write(key, v);
        }
    }
}
