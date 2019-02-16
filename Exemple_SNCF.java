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

public class SNCF {

  public static class TokenizerMapperA 
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable mois = new IntWritable();
    private Text ville = new Text(); 

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String tab[] = value.toString().split(";");
      if (tab.length == 6) {
        ville.set(tab[4]);
        mois.set(Integer.parseInt(tab[0].substring(5, 7)));
        context.write(ville, mois);
      }
    }
  }


  public static class IntSumReducerA
       extends Reducer<Text,IntWritable,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int[] tab = new int[12];
      for (IntWritable val : values) {
        ++tab[val.get() - 1];
      }
      String concat = "";
      int max = tab[0];
      int maxIndex = 0;
      for (int i = 1 ; i < 12 ; i++) {
        concat += tab[i] + " ";
        if (tab[i] > max) {
          maxIndex = i;
          max = tab[i];
        }
      }
      concat += " ("+ maxIndex + ")";

      result.set(concat);
      context.write(key, result);
    }
  }

  public static class TokenizerMapperB 
       extends Mapper<Object, Text, IntWritable, Text>{

    private final static IntWritable mois = new IntWritable();
    private Text ville = new Text(); 

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String tab1[] = value.toString().split("\t");
      if (tab1.length == 2) {
        String tab2[] = tab1[1].toString().split(" ");
        if (tab2.length == 13) {
          ville.set(tab1[0]);
          mois.set(Integer.parseInt(tab2[12].replace("(","").replace(")","")));
          context.write(mois, ville);
        }
      }
    }
  }


  public static class IntSumReducerB
       extends Reducer<IntWritable,Text,IntWritable,Text> {
    private Text result = new Text();

    public void reduce(IntWritable key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      String villes = "";
      int cpt = 0;
      for (Text val : values) {
        villes += val + " ";
        cpt++;
      }
      System.out.println(key.toString() + " : (" + cpt + ") " + villes);
      result.set("(" + cpt + ") " + villes);
      context.write(key, result);
    }
  }


  public static void main(String[] args) throws Exception{
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    FileSystem fs = FileSystem.newInstance(conf);

    if (otherArgs.length != 2) {
      System.err.println("Usage: SNCF <in> <out>");
      System.exit(2);
    }
    Job jobA = new Job(conf, "SNCF");
    jobA.setJarByClass(SNCF.class);

    jobA.setMapperClass(TokenizerMapperA.class);
    jobA.setMapOutputKeyClass(Text.class);
    jobA.setMapOutputValueClass(IntWritable.class);

    jobA.setReducerClass(IntSumReducerA.class);
    jobA.setOutputKeyClass(Text.class);
    jobA.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(jobA, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(jobA, new Path(otherArgs[1]));
    jobA.waitForCompletion(true);

    Job jobB = new Job(conf, "SNCF");
    jobB.setJarByClass(SNCF.class);

    jobB.setMapperClass(TokenizerMapperB.class);
    jobB.setMapOutputKeyClass(IntWritable.class);
    jobB.setMapOutputValueClass(Text.class);

    jobB.setReducerClass(IntSumReducerB.class);
    jobB.setOutputKeyClass(IntWritable.class);
    jobB.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(jobB, new Path(otherArgs[1]+"/part-r-00000"));
    FileOutputFormat.setOutputPath(jobB, new Path(otherArgs[1]+"-final"));
    System.exit(jobB.waitForCompletion(true) ? 0 : 1);
  }
}
