package Weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingOfYear extends WritableComparator {
    public GroupingOfYear(){
        super(WeatherWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        WeatherWritable w1 = (WeatherWritable) a;
        WeatherWritable w2 = (WeatherWritable) b;
        //分组
        return Integer.compare(w1.getYear(), w2.getYear());
    }
}
