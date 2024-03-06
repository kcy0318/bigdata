package WordCount;

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

public class WordCountRun {
    public static void main(String[] args){
        try {
            //
            Configuration conf = new Configuration();
            //
            FileSystem hdfs = FileSystem.get(conf);
            //
            String input = "/books";
            //
            String output = "/wordcount_output";
            Path outputPath = new Path(output);
            //判断输出路径是否存在，存在则删除之
            if(hdfs.exists(outputPath)){
                hdfs.delete(outputPath, true);
            }
            //创建Job任务
            Job job = Job.getInstance(conf, "word count");
            //设置运行类
            job.setJarByClass(WordCountRun.class);
            //设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, input);
            //设置Mapper
            job.setMapperClass(WordCountMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            //设置归并
            //job.setCombinerClass(WordCountCombiner.class);
            //设置分区
            job.setNumReduceTasks(3);
            job.setPartitionerClass(WordCountPartitioner.class);
            //设置Reducer
            job.setReducerClass(WordCountReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            //设置输出
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, outputPath);
            //设置运行
            boolean flag = job.waitForCompletion(true);
            //提示信息
            if(flag){
                System.out.println("词频统计结束……");
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
