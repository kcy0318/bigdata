package Weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TemperatureOfDesc extends WritableComparator {
    public TemperatureOfDesc() {
        super(WeatherWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 自定义比较、自定义排序
        WeatherWritable w1 = (WeatherWritable) a;
        WeatherWritable w2 = (WeatherWritable) b;
        //年份不同，比较年份
        if (w1.getYear() != w2.getYear()) {
            return Integer.compare(w1.getYear(), w2.getYear());
        }
        //年份相同，温度降序
        return -Float.compare(w1.getTemperature(), w2.getTemperature());
    }
}
