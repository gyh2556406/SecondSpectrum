import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.ToolRunner;
import parse.stockParseMR;

import java.io.*;
import java.net.URI;

/**
 * Created by gyhale on 15/11/9.
 */
public class Main {
    public static void main(String[] args) throws IOException{
        Configuration conf = new Configuration();
        String[] params = new String[2];
        params[0] = "./values.csv";
        params[1] = "/result";

        int res = -1;
        try {
            res = ToolRunner.run(conf, new stockParseMR(), params);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("Success or not:" + res);
        if(res == 1){
            String uri = "hdfs://localhost:9000/result/ResultForParse/part-r-00000";
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            InputStream in = fs.open(new Path(uri));
            BufferedReader bf = new BufferedReader(new InputStreamReader(in));
            String stringRead ;
            String key = "";
            float maxDeltaValue = 0;
            while((stringRead = bf.readLine()) != null){

                String[] stringReadSplit = stringRead.split("\\t");
                if(maxDeltaValue == 0){
                    key = stringReadSplit[0];
                    maxDeltaValue = Float.parseFloat(stringReadSplit[1]);
                }else{
                    float currentDeltaValue = Float.parseFloat(stringReadSplit[1]);
                    if(currentDeltaValue > maxDeltaValue){
                        key = stringReadSplit[0];
                        maxDeltaValue = currentDeltaValue;
                    }
                }
            }
            System.out.println(key + "\t" + maxDeltaValue);
        }
    }
}
