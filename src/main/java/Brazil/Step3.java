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

public class Step3 {
    private static class Step3Mapper extends Mapper<LongWritable, Text, Text, WeatherWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, WeatherWritable>.Context context) throws IOException, InterruptedException {
            //83377_01/01/1963 0.0 29.0 16.7 21.74
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
            String city = city_date.split("_")[0];
            String year = city_date.split("_")[1].substring(6);
            double rainfall = Double.parseDouble(items[1]);
            double maxTemperature = Double.parseDouble(items[2]);
            double minTemperature = Double.parseDouble(items[3]);
            double avgTemperature = Double.parseDouble(items[4]);
            //实例化对象
            WeatherWritable w = new WeatherWritable(city_date, rainfall, maxTemperature, minTemperature, avgTemperature);
            //输出：（“83377_1963”，w）
            context.write(new Text(city + "_" + year), w);
        }
    }

    private static class Step3Reducer extends Reducer<Text, WeatherWritable, Text, NullWritable> {
        private static boolean IS_PRINTED = false;
        private static final String TITLE = "城市与日期\t降雨天数\t最高温度\t最低温度\t平均温度";

        @Override
        protected void reduce(Text key, Iterable<WeatherWritable> values, Reducer<Text, WeatherWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            if (!IS_PRINTED) {
                context.write(new Text(TITLE), NullWritable.get());
                IS_PRINTED = !IS_PRINTED;
            }

            //实例化对象：这个对象存储每年的最高温度、每年的最低温度、每年的平均温度、每年的降雨天数
            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;
            double avg = 0;
            int days = 0;
            //求每年的最高温度，每年的最低温度，每年的平均温度，每年的降雨天数
            for (WeatherWritable w : values) {
                //最高温度
                max = Double.max(max, w.getMaxTemperature());
                //最低温度
                min = Double.min(min, w.getMinTemperature());
                //平均温度
                avg = w.getAvgTemperature();
                //降雨天数
                if (w.getRainfall() != 0) {
                    days++;
                }
            }
            //输出
            context.write(new Text(key.toString() + "\t" + days + "\t" + "    " + max + "\t" + "    " + min + "\t" + "    " + avg), NullWritable.get());
        }
    }

    public static void run(String input, String output) {
        try {
            Configuration conf = HadoopUtils.getConf();
            //获取HDFS对象
            FileSystem hdfs = HadoopUtils.getHdfs();
            Path outputPath = new Path(output);
            //判断输出路径是否存在，存在则删除之
            if (hdfs.exists(outputPath)) {
                hdfs.delete(outputPath, true);
            }
            //创建Job任务
            Job job = Job.getInstance(conf, "step3");
            job.setJarByClass(Step1.class);
            //设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, input);
            //设置Mapper
            job.setMapperClass(Step3Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(WeatherWritable.class);
            //设置Reducer
            job.setReducerClass(Step3Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            //设置输出
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, outputPath);
            //运行
            boolean flag = job.waitForCompletion(true);
            //提示信息
            if (flag) {
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
