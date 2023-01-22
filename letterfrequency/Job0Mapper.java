import java.lang.*;
import java.util.Arrays;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Job0Mapper: remove the punctuations from the lines
 * Start and stop adding lines based where the book starts
 * so as to ignore the metadata lines in the books
 */
public class Job0Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {

  private String startText = new String("*** START OF THIS PROJECT GUTENBERG EBOOK");
  private String endText = new String("*** END OF THIS PROJECT GUTENBERG EBOOK");
  private static boolean startProcessing = false;

  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    String s = value.toString();
    if (s.contains(startText)) {
      startProcessing = true;
    } else if (s.contains(endText)) {
      startProcessing = false;
    }


    if (startProcessing) {
      s = s.replaceAll("\\p{Punct}",""); // remove all punctuation [!"#$%&'()*+,-./:;?@[\]^_`{|}~ ] 
      s = s.replaceAll("\\d", ""); // remove all [0-9]
      s = s.trim(); // trim anything else of the line
      s = s.toLowerCase(); // string needs to be lowercase in order to compare to online tables
      System.out.println(" Line:" + key);
      if (s.length() > 0) {
        System.out.println(" Line:" + s);
        context.write(key, new Text(s));
      }
    }
  }
}
