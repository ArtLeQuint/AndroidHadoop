import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q3 {

  static final int APP      = 0,
                 CATEGORY = 1,
                 RATING   = 2,
                 REVIEW   = 3,
                 SIZE     = 4,
                 INSTALLS = 5,
                 TYPE     = 6,
                 PRICE    = 7,
                 TPUBLIC  = 8,
                 GENRE    = 9;

  static final String[] RANGE = {"$0.01 - $9.99", 
                                  "$10.00 - $19.99", 
                                  "$20.00 - $29.99", 
                                  "$30.00 +"};

  public static class TokenizerMapperA
       extends Mapper <Object, Text, Text, IntWritable> {

    private final static IntWritable revenue = new IntWritable();

    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {

      String str_price = value.toString().split(";")[PRICE];
      String str_installs = value.toString().split(";")[INSTALLS];
      String my_tranche = "";

      float price; // true price
      float installs; //installations number

      str_price = str_price.replace("$", "");

      try{
        price = Float.parseFloat(str_price); 
      } catch(NumberFormatException e){
        return;
      }
    
      if (price > 0 && price < 10){
        my_tranche = RANGE[0];
      } else if (price >= 10 && price < 20){
        my_tranche = RANGE[1];
      } else if (price >= 20 && price < 30){
        my_tranche = RANGE[2];
      } else if (price >= 30){
        my_tranche = RANGE[3];
      }

      if (my_tranche == "")
        return;

      //determine installations number
      str_installs = str_installs.replace("+", "");
      str_installs = str_installs.replace(".", "");

      boolean million = str_installs.contains("M");
      str_installs.replace("M", "");
      installs = (million) ? Integer.valueOf(str_installs) * 1000000 : Integer.valueOf(str_installs);

      revenue.set(Math.round(installs * price));

      // DEBUG
      //System.out.println("message :");
      //System.out.println("MAP : (" + my_tranche + " , " + String.valueOf(revenue) + " )");
      context.write(new Text(my_tranche), revenue);
    }

  }


  public static class IntSumReducerA
    extends Reducer <Text,IntWritable,Text,Text> {

    private Text textresult = new Text();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      float result = 0;
      for(IntWritable value : values)
        result += value.get();

      // DEBUG 
      //System.out.println("reduce : " + key.toString() + ", value : " + String.valueOf(result));
      
      textresult.set(String.valueOf("$"+Math.round(result)));
      context.write(key, textresult);
    }
  }


  public static void main(String[] args) throws Exception{

    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    FileSystem fs = FileSystem.newInstance(conf);

    if (otherArgs.length != 2) {
      System.err.println("Usage: Q3 <in> <out>");
      System.exit(2);
    }
    Job jobA = new Job(conf, "Q3");
    jobA.setJarByClass(Q3.class);

    jobA.setMapperClass(TokenizerMapperA.class);
    jobA.setMapOutputKeyClass(Text.class);
    jobA.setMapOutputValueClass(IntWritable.class);

    jobA.setReducerClass(IntSumReducerA.class);
    jobA.setOutputKeyClass(Text.class);
    jobA.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(jobA, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(jobA, new Path(otherArgs[1]));

    jobA.waitForCompletion(true);

    System.exit(jobA.waitForCompletion(true) ? 0 : 1);
  }
}
