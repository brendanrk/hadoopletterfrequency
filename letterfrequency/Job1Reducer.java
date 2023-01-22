import java.io.IOException;
import java.text.DecimalFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;

/**
 * Job1Reducer: Takes the aggregated counts and divides by total to get 
 * percentage values on the output
 */
public class Job1Reducer extends Reducer<TextPair, IntWritable, TextPair, Text> {

  private long mapperCounter_de;
  private long mapperCounter_es;
  private long mapperCounter_it;
  private long mapperCounter_fr;
  private static final DecimalFormat df = new DecimalFormat("000.000");

  @Override
  public void setup(Context context) throws IOException, InterruptedException{
      Configuration conf = context.getConfiguration();
      Cluster cluster = new Cluster(conf);
      Job currentJob = cluster.getJob(context.getJobID());
      mapperCounter_es = currentJob.getCounters().findCounter(Job1Mapper.Counters.TOTAL_LETTERS_ES).getValue();  
      mapperCounter_fr = currentJob.getCounters().findCounter(Job1Mapper.Counters.TOTAL_LETTERS_FR).getValue();  
      mapperCounter_it = currentJob.getCounters().findCounter(Job1Mapper.Counters.TOTAL_LETTERS_IT).getValue();  
      mapperCounter_de = currentJob.getCounters().findCounter(Job1Mapper.Counters.TOTAL_LETTERS_DE).getValue();  
  }

  @Override
  public void reduce(TextPair key, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {
    int wordCount = 0;
    for (IntWritable value : values) {
      wordCount += value.get();
    }
    System.out.println(" In Job1Reducer now! key-letter:" + key.toString());
    System.out.println(" In Job1Reducer now! key only:" + key.getFirst().toString());
    System.out.println(" In Job1Reducer now! wordCount:" + wordCount);

    String wordperc;
    long mapperCounter = -99999;
    String keylang = key.getFirst().toString();
    // Switch statement for language
    switch (keylang) {
      // french
      case "fr":
        wordperc = df.format(((double) wordCount / mapperCounter_fr)*100);
        mapperCounter = mapperCounter_fr;
        break;
      // German
      case "de":
        wordperc = df.format(((double) wordCount / mapperCounter_de)*100);
        mapperCounter = mapperCounter_de;
        break;
      // Spanish
      case "es":
        wordperc = df.format(((double) wordCount / mapperCounter_es)*100);
        mapperCounter = mapperCounter_es;
        break;
      // Italian
      case "it":
        wordperc = df.format(((double) wordCount / mapperCounter_it)*100);
        mapperCounter = mapperCounter_it;
        break;
      default:
        wordperc = "-99999";
        System.out.println("no match");
    }
    System.out.println(" In Job1Reducer now! mapperCounter:" + mapperCounter);
    System.out.println(" In Job1Reducer now! wordperc:" + wordperc);
    context.write(key, new Text(wordperc));
  }
}
