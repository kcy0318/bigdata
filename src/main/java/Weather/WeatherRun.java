package Weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class WeatherRun {
    public static void main(String[] args){
        try {
            //实例化配置对象：若存在xml文件时，自动读取配置，使用xml文件时，无需设置hadoop集群属性
            Configuration conf = new Configuration();
            //获取HDFS对象
            FileSystem hdfs = FileSystem.get(conf);
            //定义输入路径
            String input = "/weather";
            //定义输出路径
            String output = "/weather_output";
            Path outputPath = new Path(output);
            //判断输出路径是否存在，存在则删除之
            if(hdfs.exists(outputPath)){
                hdfs.delete(outputPath, true);
            }
            //创建Job任务
            Job job = Job.getInstance(conf, "weather");
            //设置运行类
            job.setJarByClass(WeatherRun.class);
            //设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, input);
            //设置Mapper
            job.setMapperClass(WeatherMapper.class);
            job.setMapOutputKeyClass(WeatherWritable.class);
            job.setMapOutputValueClass(Text.class);
            //设置归并
            //job.setCombinerClass(WordCountCombiner.class);
            //设置分区
            job.setNumReduceTasks(3);
            job.setPartitionerClass(WeatherPartitioner.class);
            //设置Reducer
            job.setReducerClass(WeatherReducer.class);
            job.setOutputKeyClass(WeatherWritable.class);
            job.setOutputValueClass(Text.class);
            //设置输出
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, outputPath);
            //设置运行
            boolean flag = job.waitForCompletion(true);
            //提示信息
            if(flag){
                System.out.println("每年最高温度统计结束……");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
