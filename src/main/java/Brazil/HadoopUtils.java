package Brazil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public abstract class HadoopUtils {
    private static Configuration conf;
    private static FileSystem hdfs;
    static {
        try {
            conf = new Configuration();
            hdfs = FileSystem.get(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Configuration getConf() {
        return conf;
    }

    public static FileSystem getHdfs() {
        try {
            if (hdfs == null) {
                hdfs = FileSystem.get(conf);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return hdfs;
    }

    public static void showDirectoryFileContents(String path) {
        try {
            Path target = new Path(path);
            if (!hdfs.exists(target)) {
                System.out.println("目标路径不存在~~~");
                return;
            }
            //遍历指定目录下所有子目录与文件
            for (FileStatus s : hdfs.listStatus(target)) {
                //判断是否是文件；若是文件，则读取文件内容
                if (s.isFile()) {
                    //构建文件输入流
                    FSDataInputStream fsDataInputStream = hdfs.open(s.getPath());
                    //构建读取器
                    InputStreamReader r = new InputStreamReader(fsDataInputStream);
                    //构建缓冲读取器
                    BufferedReader reader = new BufferedReader(r);
                    //循坏按行读取
                    reader.lines().forEach(lr -> {
                        System.out.println(lr);
                    });
                    //关闭流
                    reader.close();
                    r.close();
                    fsDataInputStream.close();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
