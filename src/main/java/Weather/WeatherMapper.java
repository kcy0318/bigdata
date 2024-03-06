package Weather;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WeatherMapper extends Mapper<LongWritable, Text, WeatherWritable, Text>{
    @Override
    protected void map(LongWritable key, Text value,Mapper<LongWritable, Text, WeatherWritable, Text>.Context context) throws IOException, InterruptedException {
        //1949-10-01 14:21:02 34℃
        //读取数据
        String line = value.toString();
        //数据清洗与验证
        if (StringUtils.isBlank(line)){
            return;
        }
        //拆分
        String[] items = line.split("\t");
        //验证
        if (items.length != 2) {
            return;
        }
        //取年份、温度
        int year = Integer.parseInt(items[0].substring(0,4));
        float temperature = Float.parseFloat(items[1].substring(0,items[1].lastIndexOf("℃")));
        //定义实体对象
        WeatherWritable w = new WeatherWritable(year,temperature);
        //输出
        context.write(w, value);
    }
}
