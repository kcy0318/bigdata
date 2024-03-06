package Weather;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WeatherPartitioner extends Partitioner<WeatherWritable, Text> {
    @Override
    public int getPartition(WeatherWritable key, Text text, int numPartitions) {
        // 分区算法：简单；目的：让相同年份进入同一个reduce处理
        // 1949-1940 =  9%3 = 0
        // 1950-1940 = 10%3 = 1
        // 1951-1940 = 11%3 = 2
        return (key.getYear() - 1940) % numPartitions;
    }
}
