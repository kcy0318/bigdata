package Brazil;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class WeatherDBWritable implements WritableComparable<WeatherDBWritable>, DBWritable {
    private String city_date;
    private double rainfall;
    private double maxTemperature;
    private double minTemperature;
    private double avgTemperature;
    public WeatherDBWritable() {
    }
    public WeatherDBWritable(String city_date, double rainfall, double maxTemperature, double minTemperature, double avgTemperature) {
        this.city_date = city_date;
        this.rainfall = rainfall;
        this.maxTemperature = maxTemperature;
        this.minTemperature = minTemperature;
        this.avgTemperature = avgTemperature;
    }

    @Override
    public String toString() {
        return city_date +
                "\t" + rainfall +
                "\t" + maxTemperature +
                "\t" + minTemperature +
                "\t" + avgTemperature;
    }

    @Override
    public int compareTo(WeatherDBWritable other) {
        if (null == other) {
            return 1;
        }
        //默认按照城市与日期升序排列
        return this.city_date.compareTo(other.city_date);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.city_date);
        dataOutput.writeDouble(this.rainfall);
        dataOutput.writeDouble(this.maxTemperature);
        dataOutput.writeDouble(this.minTemperature);
        dataOutput.writeDouble(this.avgTemperature);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.city_date = dataInput.readUTF();
        this.rainfall = dataInput.readDouble();
        this.maxTemperature = dataInput.readDouble();
        this.minTemperature = dataInput.readDouble();
        this.avgTemperature = dataInput.readDouble();
    }

    public String getCity_date() {
        return city_date;
    }

    public void setCity_date(String city_date) {
        this.city_date = city_date;
    }

    public double getRainfall() {
        return rainfall;
    }

    public void setRainfall(double rainfall) {
        this.rainfall = rainfall;
    }

    public double getMaxTemperature() {
        return maxTemperature;
    }

    public void setMaxTemperature(double maxTemperature) {
        this.maxTemperature = maxTemperature;
    }

    public double getMinTemperature() {
        return minTemperature;
    }

    public void setMinTemperature(double minTemperature) {
        this.minTemperature = minTemperature;
    }

    public double getAvgTemperature() {
        return avgTemperature;
    }

    public void setAvgTemperature(double avgTemperature) {
        this.avgTemperature = avgTemperature;
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        //写入数据库
        statement.setString(1, this.city_date);
        statement.setString(2, this.city_date);
        statement.setString(3, this.city_date);
        statement.setString(4, this.city_date);
        statement.setString(5, this.city_date);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        //读取数据库
        this.city_date = resultSet.getString(1);
        this.city_date = resultSet.getString(2);
        this.city_date = resultSet.getString(3);
        this.city_date = resultSet.getString(4);
        this.city_date = resultSet.getString(5);
    }
}
