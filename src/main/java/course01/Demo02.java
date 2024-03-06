package course01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
public class Demo02 {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        Path target = new Path("/myhdfs/demo02.txt");
        FSDataOutputStream fsDataOutputStream = hdfs.create(target, true);
        String line = "I love China";
        fsDataOutputStream.write(line.getBytes());
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
    }
}
