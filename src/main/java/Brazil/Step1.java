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

public class Step1{
    private static class Step1Mapper extends Mapper<LongWritable, Text, Text, WeatherWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, WeatherWritable>.Context context) throws IOException, InterruptedException {
            // Estacao;Data;Hora;Precipitacao;TempBulboSeco;TempBulboUmido;
            // TempMaxima;TempMinima;UmidadeRelativa;PressaoAtmEstacao;
            // PressaoAtmMar;DirecaoVento;VelocidadeVento;Insolacao;
            // Nebulosidade;Evaporacao Piche;Temp Comp Media;
            // Umidade Relativa Media;Velocidade do Vento Media;
            //82024;01/01/1961;0000;;;;32.3;;;;;;;4.4;;;26.56;82.5;3;
            //82024;01/01/1961;1200;;26;23.9;;22.9;83;994.2;;5;5;;8;;;;;
            //82024;01/01/1961;1800;;32.3;27;;;65;991.6;;5;3;;9;;;;;
            String line = value.toString();
            //空数据
            if (StringUtils.isEmpty(line)) {
                return;
            }
            //忽略标题
            if (line.startsWith("Estacao")) {
                return;
            }
            //仅选择首都数据，巴西利亚，83377
            if (!line.startsWith("83377")) {
                return;
            }
            //拆分数据
            String[] items = line.split(";", 19);
            //忽略因块的拆分导致不在一个块的行
            if(items.length != 19) {
                return;
            }
            //提取
            String city_date = items[0] + "_" + items[1];
            double rainfall = Double.parseDouble((StringUtils.isEmpty(items[3]) ? "0" : items[3]));
            double maxTemperature = Double.parseDouble((StringUtils.isEmpty(items[6]) ? "0" : items[6]));
            double minTemperature = Double.parseDouble((StringUtils.isEmpty(items[7]) ? "0" : items[7]));
            double avgTemperature = Double.parseDouble((StringUtils.isEmpty(items[16]) ? "0" : items[16]));
            //构建对象
            WeatherWritable w = new WeatherWritable(city_date, rainfall, maxTemperature, minTemperature, avgTemperature);
            //输出("83377_01/01/1961", w)
            context.write(new Text(city_date), w);
        }
    }
    private static class Step1Reducer extends Reducer<Text, WeatherWritable, WeatherWritable, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<WeatherWritable> values, Reducer<Text, WeatherWritable, WeatherWritable, NullWritable>.Context context) throws IOException, InterruptedException {
            WeatherWritable w = new WeatherWritable();
            w.setCity_date(key.toString());
            for (WeatherWritable v : values) {
                w.setRainfall(w.getRainfall() + v.getRainfall());
                w.setMaxTemperature(w.getMaxTemperature() + v.getMaxTemperature());
                w.setMinTemperature(w.getMinTemperature() + v.getMinTemperature());
                w.setAvgTemperature(w.getAvgTemperature() + v.getAvgTemperature());
            }
            //判断数据合法性
            if (w.getMaxTemperature() <= 0 || w.getMinTemperature() <= 0 || w.getAvgTemperature() <= 0) {
                return;
            }
            //输出
            context.write(w, NullWritable.get());
        }
    }
    public static void run(String input, String output){
        try {
            //
            Configuration conf = HadoopUtils.getConf();
            //
            FileSystem hdfs = HadoopUtils.getHdfs();
            //
            //String input = "/Brazil_weather";
            //
            //String output = "/Step1_output";
            Path outputPath = new Path(output);
            //判断输出路径是否存在，存在则删除之
            if(hdfs.exists(outputPath)){
                hdfs.delete(outputPath, true);
            }
            //创建Job任务
            Job job = Job.getInstance(conf, "step1");
            //设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, input);
            //设置Mapper
            job.setMapperClass(Step1Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(WeatherWritable.class);
            //设置Reducer
            job.setReducerClass(Step1Reducer.class);
            job.setOutputKeyClass(WeatherWritable.class);
            job.setOutputValueClass(NullWritable.class);
            //设置输出
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, outputPath);
            //设置运行
            boolean flag = job.waitForCompletion(true);
            //提示信息
            if(flag){
                System.out.println("天气数据清洗与导入结束……");
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


