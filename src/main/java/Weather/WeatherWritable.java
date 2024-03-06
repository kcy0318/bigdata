package Weather;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WeatherWritable implements WritableComparable<WeatherWritable> {
    private int year;
    private float temperature;

    public WeatherWritable(){

    }

    public WeatherWritable(int year, float temperature){
        this.year = year;
        this.temperature = temperature;
    }

    @Override
    public int compareTo(WeatherWritable other) {
        // 默认比较、默认排序
        // 返回值含义：1或>0代表是大，0代表是相等，<0或-1代表是小
        // 判断空
        if (null == other){
            return 1;
        }
        //年份不同，比较年份
        if (this.year != other.year){
            return  Integer.compare(this.year, other.year);
        }
        //年份相同，比较温度
        return Float.compare(this.temperature, other.temperature);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        //序列化
        out.writeInt(this.year);
        out.writeFloat(this.temperature);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.temperature = in.readFloat();
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public float getTemperature(){
        return temperature;
    }

    public void setTemperature(float temperature){
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return year + "\t" + temperature;
    }
}
