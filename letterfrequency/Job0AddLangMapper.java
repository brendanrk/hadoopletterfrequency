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
 * Job0AddLangMapper: add the <lang> key to the output along with the line 
 * from Job0Mapper
 */
public class Job0AddLangMapper extends Mapper<LongWritable, Text, Text, Text> {

  private String startText = new String("*** START OF THIS PROJECT GUTENBERG EBOOK");
  private String endText = new String("*** END OF THIS PROJECT GUTENBERG EBOOK");
  private static boolean startProcessing = false;

  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    /*
        Split the filepath to get the language, Exmaple:
        hdfs://localhost:9000/user/hduser/gutenberg/de/57909.txt
        [hdfs:, , localhost:9000, user, hduser, gutenberg, de, 57909.txt]
        de 
    */
    Path filePath = ((FileSplit) context.getInputSplit()).getPath();
    String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
    // System.out.println(filePathString);
    String[] items= filePathString.split("/");
    // System.out.println(Arrays.toString(items));
    String lang = items[items.length - 2];
    // System.out.println(lang);

    context.write(new Text(lang), value);

  }
}
