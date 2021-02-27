import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsTest {
    public static final String inputPath = "hdfs://192.168.8.100:9000/user/datapros/test/file";
    public static final String inputmsg = "hello~~\n";

    public static void main(String [] args) throws IOException {

        // hadoop의 configuration을 생성
        Configuration config = new Configuration();
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        config.set("dfs.client.use.datanode.hostname", "true");
        config.set("hadoop.root.logger", "DEBUG");

        Path filenamePath = new Path(inputPath);

        // config를 HDFS로 parse
        FileSystem fs = filenamePath.getFileSystem(config);

        try {
            // inputmsg를 HDFS에 write
            System.out.println("Creating....");
            FSDataOutputStream fin = fs.create(filenamePath);
            System.out.println("Writing....");
            fin.writeUTF(inputmsg);
            System.out.println("Closing....");
            fin.close();

            // filenamePath file을 읽어들임
            System.out.println("Opening....");
            FSDataInputStream fout = fs.open(filenamePath);
            System.out.println("Reading....");
            String msgIn = fout.readUTF();

            // 콘솔창에 출력
            System.out.println(msgIn);

            System.out.println("Closing....");
            fout.close();
            fs.close();

        } catch (IOException ioe) {
            System.err.println("IOException during operation " + ioe.toString());
            System.exit(1);
        }
    }
}
