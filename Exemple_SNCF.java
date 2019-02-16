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

public class Exemple_SNCF {

  // Premier map
  public static class TokenizerMapperA 
       extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable mois = new IntWritable();
    private Text ville = new Text(); 

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      String tab[] = value.toString().split(";");
      if (tab.length == 6) {
        // Pour chaque ville, on écris quelque chose de type
        // Strasbourg : 1 1 1 1 1 1 3 12 12
        // dans cette exemple on a trouvé 6 objets à Strasbourg perdus en janvier, 2 en décembre et 1 en mars
        ville.set(tab[4]);
        mois.set(Integer.parseInt(tab[0].substring(5, 7)));
        context.write(ville, mois);
      }
    }
  }

  // Premier Reduce
  public static class IntSumReducerA
       extends Reducer<Text,IntWritable,Text,Text> {

    private Text result = new Text();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {

      // Ici on veut transformer le tableau précédent en :
      // Abbeville 2 2 7 3 6 5 5 0 1 0 2 (3)
      // qui nous dit qu'à Abbeville on a perdu 2 objets en janviers, 2 en février, 7 en mars... etc
      // le dernier nombre entre parenthèse est l'index du mois ou il y a le plus de pertes (Mars)

      // Je prends un nouveau tableau de mois
      int[] tab = new int[12];
      // Je compte le nombre de fois que je vois chaque mois 
      for (IntWritable val : values) {
        ++tab[val.get() - 1];
      }
      String concat = "";
      int max = tab[0];
      int maxIndex = 0;
      // Je cherche le mois ou il y a le max de perte
      for (int i = 1 ; i < 12 ; i++) {
        concat += tab[i] + " ";
        if (tab[i] > max) {
          maxIndex = i;
          max = tab[i];
        }
      }
      concat += " ("+ maxIndex + ")";

      // Je met ce mois dans un tableau de type
      // ville : mois_avec_le_plus_de_perte
      result.set(concat);
      context.write(key, result);
    }
  }

  // Second map
  public static class TokenizerMapperB 
       extends Mapper<Object, Text, IntWritable, Text> {

    private final static IntWritable mois = new IntWritable();
    private Text ville = new Text(); 

    public void map(Object key, Text value, Context context) 
      throws IOException, InterruptedException {
      
      // le fichier créé nous force à split via une tabulation pour séparer le nom de la ville de ses stats
      // rappel : value = "Abbeville  2 2 7 3 6 5 5 0 1 0 2 (3)"
      String tab1[] = value.toString().split("\t");
      if (tab1.length == 2) {
        // On prend la deuxieme partie et on split par espace pour avoir le score de chaque mois et le max en 13eme
        String tab2[] = tab1[1].toString().split(" ");
        if (tab2.length == 13) {
          // Nom de la ville
          ville.set(tab1[0]);
          // Mois avec le plus de pertes
          mois.set(Integer.parseInt(tab2[12].replace("(","").replace(")","")));
          context.write(mois, ville);
        }
      }
    }
  }

  // Second reduce
  public static class IntSumReducerB
       extends Reducer<IntWritable,Text,IntWritable,Text> {

    private Text result = new Text();

    public void reduce(IntWritable key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {

      // Ici on veut obtenir un tableau de la forme
      // 5 (8) Nantes Yvetot Cherbourg Vernon Châlons-en-Champagne Chartres Paris-Est Montluçon
      // qui nous dit qu'au mois en index 5 (donc 6eme mois car index commence a 0 donc Juillet)
      // ces villes perdent leur plus grand nombre d'objets dans les trains

      String villes = "";
      int cpt = 0;
      for (Text val : values) {
        // Ici je dit juste d'accoler les villes dans une string et pour chacune j'incrémente le cpt
        villes += val + " ";
        cpt++;
      }
      // Ca c'était du debug, ça sert a rien
      System.out.println(key.toString() + " : (" + cpt + ") " + villes);

      // La je met en place le resultat
      // La clé key, c'est le mois du coup
      result.set("(" + cpt + ") " + villes);
      context.write(key, result);
    }
  }


// La c'est le main, qui gère le lancement de chaque job
  public static void main(String[] args) throws Exception {
    // Ca c'est la base
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    FileSystem fs = FileSystem.newInstance(conf);

    // La partie aide à l'utilisateur
    if (otherArgs.length != 2) {
      System.err.println("Usage: Exemple_SNCF <in> <out>");
      System.exit(2);
    }
    // Lancement d'un premier job
    Job jobA = new Job(conf, "Exemple_SNCF");
    jobA.setJarByClass(Exemple_SNCF.class);

    jobA.setMapperClass(TokenizerMapperA.class);
    jobA.setMapOutputKeyClass(Text.class);
    jobA.setMapOutputValueClass(IntWritable.class);

    jobA.setReducerClass(IntSumReducerA.class);
    jobA.setOutputKeyClass(Text.class);
    jobA.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(jobA, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(jobA, new Path(otherArgs[1]));

    // attente de la fin de ce job
    jobA.waitForCompletion(true);

    // Lancement d'un deuxieme job
    Job jobB = new Job(conf, "Exemple_SNCF");
    jobB.setJarByClass(Exemple_SNCF.class);

    jobB.setMapperClass(TokenizerMapperB.class);
    jobB.setMapOutputKeyClass(IntWritable.class);
    jobB.setMapOutputValueClass(Text.class);

    jobB.setReducerClass(IntSumReducerB.class);
    jobB.setOutputKeyClass(IntWritable.class);
    jobB.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(jobB, new Path(otherArgs[1]+"/part-r-00000"));
    FileOutputFormat.setOutputPath(jobB, new Path(otherArgs[1]+"-final"));

    // Attente du job et fin du programme
    System.exit(jobB.waitForCompletion(true) ? 0 : 1);
  }
}
