package Brazil;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Scanner;

public class Step2 {
    private static class Step2Mapper extends Mapper<LongWritable, Text, WeatherWritable, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value,Mapper<LongWritable, Text, WeatherWritable, NullWritable>.Context context ) throws IOException, InterruptedException {
            //获取查询日期
            String search_date = context.getConfiguration().get("search_date");
            if (StringUtils.isBlank(search_date)) {
                return;
            }
            //83377_01/01/1963 0.0 29.0 16.7 21.64
            String line = value.toString();
            if (StringUtils.isBlank(line)) {
                return;
            }
            //解析字符串成一个实体对象
            String[] items = line.split("\t");
            if (items.length != 5) {
                return;
            }
            //解析
            String city_date = items[0];
            //筛选日期
            if (!search_date.equals(city_date.split("_")[1])) {
                return;
            }
            double rainfall = Double.parseDouble(items[1]);
            double maxTemperature = Double.parseDouble(items[2]);
            double minTemperature = Double.parseDouble(items[3]);
            double avgTemperature = Double.parseDouble(items[4]);
            //实例化对象
            WeatherWritable w = new WeatherWritable(city_date, rainfall, maxTemperature, minTemperature, avgTemperature);
            //输出
            context.write(w, NullWritable.get());
        }
    }

    private static class Step2Reducer extends Reducer<WeatherWritable, NullWritable, WeatherWritable, NullWritable> {
        @Override
        protected void reduce(WeatherWritable key, Iterable<NullWritable> values, Reducer<WeatherWritable, NullWritable, WeatherWritable, NullWritable>.Context context) throws IOException, InterruptedException {
            //输出
            context.write(key, NullWritable.get());
        }
    }

    public static void run(String input, String output) {
        try {
            Configuration conf = HadoopUtils.getConf();
            //输入查询的历史日期
            Scanner scanner = new Scanner(System.in);
            System.out.print("请输入查询的历史日期（MM/dd/yyyy）：");
            String search_date = scanner.next();
            //将查询日期存储至配置对象中
            conf.set("search_date", search_date);
            //获取HDFS对象
            FileSystem hdfs = HadoopUtils.getHdfs();
            Path outputPath = new Path(output);
            //判断输出路径是否存在，存在则删除之
            if (hdfs.exists(outputPath)) {
                hdfs.delete(outputPath, true);
            }
            //创建Job任务
            Job job = Job.getInstance(conf, "step2");
            job.setJarByClass(Step1.class);
            //设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, input);
            //设置Mapper
            job.setMapperClass(Step2Mapper.class);
            job.setMapOutputKeyClass(WeatherWritable.class);
            job.setMapOutputValueClass(NullWritable.class);
            //设置Reducer
            job.setReducerClass(Step2Reducer.class);
            job.setOutputKeyClass(WeatherWritable.class);
            job.setOutputValueClass(NullWritable.class);
            //设置输出
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, outputPath);
            //运行
            boolean flag = job.waitForCompletion(true);
            //提示信息
            if (flag) {
                System.out.println("城市与日期\t降雨量\t最高温度\t最低温度\t平均温度");
                HadoopUtils.showDirectoryFileContents(output);
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
