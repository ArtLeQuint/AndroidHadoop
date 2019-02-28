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

/////////////////////////////////////////////////////
// EXEMPLE SNCF COMMENTE A TRAVAILLER POUR ANDROID //
/////////////////////////////////////////////////////

public class Q1 {

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

  public static class TokenizerMapperA extends Mapper <Object, Text, Text, IntWritable> 
  {
    private final static IntWritable inst = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
      String str_genres = value.toString().split(";")[GENRE];
      String str_public = value.toString().split(";")[TPUBLIC];
      String str_rating = value.toString().split(";")[RATING];
      String str_installs = value.toString().split(";")[INSTALLS];

      String str_genre[] = str_genres.split("\\|");
      String _key[] = new String[str_genre.length];

      for(int i = 0 ; i < str_genre.length ; i++)
        _key[i] = str_genre[i] + ";" + str_public;

      float installs; //installations number


      //determine installations number
      str_installs = str_installs.replace("+", "");
      str_installs = str_installs.replace(".", "");

      boolean million = str_installs.contains("M");
      str_installs.replace("M", "");

      try{
        installs = (million) ? Integer.parseInt(str_installs) * 1000000 : Integer.parseInt(str_installs);
      }catch(NumberFormatException e){
        return;
      }

      inst.set(Math.round(installs));

      // DEBUG
      //System.out.println("message :");
      //for (String k : _key) System.out.println("MAP : (" + k + " , " + String.valueOf(installs) + " )");

      for (String k : _key)
        context.write(new Text(k), inst);
    }
  }


  public static class IntSumReducerA
    extends Reducer <Text,IntWritable,Text,Text> {

    private Text textresult = new Text();

    
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
    {
      float total = 0;
      int divider = 0;

      for(IntWritable value : values)
        total += value.get();


      // DEBUG 
      System.out.println("reduce : " + key.toString() + ", value : " + String.format ("%.0f", total));
      
      textresult.set(String.format ("%.0f", total));
      context.write(key, textresult);
    }
  }

  public static class TokenizerMapperB extends Mapper<Object, Text, Text, Text>
  {
    private final static Text _key = new Text();
    private Text val = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
      String tab1[] = value.toString().split("\t");

      if (tab1.length != 2) 
        return;

      String _keys = tab1[0];
      String genre = _keys.split(";")[0];
      String pub = _keys.split(";")[1];
      String nb_inst = tab1[1];

      // DEBUG
      //System.out.println("2nd MAP");
      //System.out.println("MAP : (" + genre + " , " + pub + "|" + nb_inst + " )");

      val.set(pub + "|" + nb_inst);
      _key.set(genre);
      context.write(_key, val);
    }
  }


  public static class IntSumReducerB extends Reducer<Text,Text,Text,Text> 
  {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
    {
      String final_inst = "; ";

      for (Text val : values) 
        final_inst += val + " ; ";
      
      // DEBUG
      //System.out.println(key.toString() + " : final_inst);

      result.set(final_inst);
      context.write(key, result);
    }
  }


  public static void main(String[] args) throws Exception{

    /* Premier passage */
    
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    FileSystem fs = FileSystem.newInstance(conf);

    if (otherArgs.length != 2) {
      System.err.println("Usage: Q1 <in> <out>");
      System.exit(2);
    }
    Job jobA = new Job(conf, "Q1");
    jobA.setJarByClass(Q1.class);

    jobA.setMapperClass(TokenizerMapperA.class);
    jobA.setMapOutputKeyClass(Text.class);
    jobA.setMapOutputValueClass(IntWritable.class);

    jobA.setReducerClass(IntSumReducerA.class);
    jobA.setOutputKeyClass(Text.class);
    jobA.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(jobA, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(jobA, new Path(otherArgs[1]));

    jobA.waitForCompletion(true);
    

    /* Second passage */

    Job jobB = new Job(conf, "Q1");
    jobB.setJarByClass(Q1.class);

    jobB.setMapperClass(TokenizerMapperB.class);
    jobB.setMapOutputKeyClass(Text.class);
    jobB.setMapOutputValueClass(Text.class);

    jobB.setReducerClass(IntSumReducerB.class);
    jobB.setOutputKeyClass(Text.class);
    jobB.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(jobB, new Path(otherArgs[1]+"/part-r-00000"));
    FileOutputFormat.setOutputPath(jobB, new Path(otherArgs[1]+"-final"));

    System.exit(jobB.waitForCompletion(true) ? 0 : 1);
  }
}
