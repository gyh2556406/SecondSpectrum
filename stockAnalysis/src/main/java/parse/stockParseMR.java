package parse;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.util.regex.*;

import java.io.IOException;

/**
 * Created by gyhale on 15/11/9.
 */
public class stockParseMR extends Configured implements Tool{
    public int run(String[] args) throws Exception {
        String inputs = args[0];
        String outputPath = args[1];

        Configuration conf = this.getConf();
        //conf.set("path",inputs);
        //conf.set("mapred.job.priority", "VERY_HIGH");
        //conf.set("mapred.child.java.opts", "-Xmx2048m");
        //conf.set("mapreduce.map.memory.mb", "1024");
        //conf.set("mapreduce.reduce.memory.mb", "4096");
//		conf.set("hadoop.tmp.dir", "/wls/applications/loki-app/tmp");

        Job job = new Job(conf, "stockParseMR" );
        job.setJarByClass(stockParseMR.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(stockParseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        job.setReducerClass(stockParseReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setNumReduceTasks(1);


        FileInputFormat.addInputPath(job, new Path(inputs));
        FileOutputFormat.setOutputPath(job, new Path(outputPath,"ResultForParse"));

        return job.waitForCompletion(true) ? 1 : 0;
    }

    public static class stockParseMapper extends Mapper<LongWritable,Text,Text,Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{
            String[] valueSplit = StringUtils.splitPreserveAllTokens(value.toString(), ",");
            Pattern p = Pattern.compile("[0-9]*\\.?[0-9]+");
            if(valueSplit.length == 5 && !valueSplit[0].equals("Name") ){
                Matcher matcher = p.matcher(valueSplit[3]);
                boolean ifMatchFlag = matcher.matches();
                if(ifMatchFlag){
                    String date = valueSplit[1];
                    String dateParse = date.replace("-","");
                    outKey.set(valueSplit[0]);
                    outValue.set(dateParse+","+valueSplit[3]);
                    context.write(outKey,outValue);
                }
            }
        }
    }
    public static class stockParseReducer extends Reducer<Text,Text,Text,FloatWritable> {
        private Text outKey = new Text();
        private FloatWritable outValue = new FloatWritable();

        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
            long maxDate = 0l;
            float maxDateValue = (float) 0.0;
            long minDate = 0l;
            float minDateValue = (float) 0.0;

            for(Text currentText : values){
                String[] currentTextSplit = currentText.toString().split(",");
                long date = Long.parseLong(currentTextSplit[0]);
                if(maxDate == 0l){
                    maxDate = date;
                    maxDateValue = Float.parseFloat(currentTextSplit[1]);
                }else{
                    if(date > maxDate){
                        maxDate = date;
                        maxDateValue = Float.parseFloat(currentTextSplit[1]);
                    }
                }

                if(minDate == 0l){
                    minDate = date;
                    minDateValue = Float.parseFloat(currentTextSplit[1]);
                }else{
                    if(date < minDate){
                        minDate = date;
                        minDateValue = Float.parseFloat(currentTextSplit[1]);
                    }
                }

            }
            float deltaValue = maxDateValue - minDateValue;
            if(deltaValue > 0){
                outKey.set(key);
                outValue.set(deltaValue);
                context.write(outKey,outValue);
            }

        }
    }
}
