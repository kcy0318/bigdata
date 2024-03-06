package course01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
public class Demo01 {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);
            Path src = new Path("E:/KCY/大学/教育大数据/Doc01.txt");
            Path dst = new Path("/myhdfs/china.txt");
            hdfs.copyFromLocalFile(false, true, src, dst);
            System.out.println("文件上传成功~~~");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
