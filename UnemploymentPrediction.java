/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package unemployementprediction;

/**
 *
 * @author keed
 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class UnemployementPrediction {
    public static int PredictionYear = 2007;

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            //setting key value as industry name
            word.set(tokenizer.nextToken());
            try {
                // Adding numbers into arraylist
                ArrayList<Double> values = new ArrayList<Double>();
                while (tokenizer.hasMoreTokens()) {
                    values.add(Double.parseDouble(tokenizer.nextToken()));
                }
                
                // calculating a and b in (y=a+bx)
                Double sigmaX = 0.0;
                Double sigmaY = 0.0;
                Double sigmaXY = 0.0;
                Double sigmaX2 = 0.0;
                Double startYear = 2000.0;
                for (Double it : values) {
                    sigmaY += it;
                    sigmaX += startYear;
                    sigmaX2 += Math.pow(startYear,2);
                    sigmaXY += it*startYear;
                    startYear++;
                }
                Double n = (double)values.size();
                Double b = ((n*sigmaXY) - (sigmaX*sigmaY))/((n*sigmaX2)- Math.pow(sigmaX, 2));
                Double a =  (sigmaY - (b*sigmaX))/n;
                
                //predicting for the year 2007
                Double prediction = a + (b*PredictionYear);
                
                //finding mean and standard deviation
                Double mean = sigmaY/n;
                Double standardDev;
                Double sum=0.0;
                for(Double it : values){
                    sum += Math.pow((it-mean),2);                    
                }
                standardDev = Math.sqrt(sum/n);
                
                // clasfying low/medium/high
                String classify;
                if( prediction < mean - (standardDev*.35)){
                    classify = "Low";
                }else if( prediction > mean + (standardDev*.35)){
                    classify = "High";
                }else {
                    classify = "Medium";
                }
                
                //CHECK OUTPUT
                //IntWritable predictionDisplay = new IntWritable((int)(double)(prediction));
                //Text classLabel = new Text("high");
                Text predictionDisplay = new Text(String.valueOf(prediction)+"\t"+classify);
                output.collect(word, predictionDisplay);

            } catch (Exception e) {
                output.collect(word, new Text("ERROR"));
            }


        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            output.collect(key, values.next());
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(UnemployementPrediction.class);
        conf.setJobName("unemploymentPrediction");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
        conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
        JobClient.runJob(conf);
    }
}
