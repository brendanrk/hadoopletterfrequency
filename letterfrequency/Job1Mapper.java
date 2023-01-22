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
 * Job1Mapper: split the line input into letters, 
 * use a Counter to count total number of letters seen
 */
public class Job1Mapper extends Mapper<Text, Text, TextPair, IntWritable> {

  static enum Counters {
    TOTAL_LETTERS_FR,
    TOTAL_LETTERS_DE,
    TOTAL_LETTERS_ES,
    TOTAL_LETTERS_IT
  }
  private static TextPair textPair = new TextPair(); 
  // using letters from https://en.wikipedia.org/wiki/Letter_frequency
  private char[] allowedletters = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'à', 'â', 'á', 'å', 'ä', 'ã', 'ą', 'æ', 'œ', 'ç', 'ĉ', 'ć', 'č', 'ď', 'ð', 'è', 'é', 'ê', 'ë', 'ę', 'ě', 'ĝ', 'ğ', 'ĥ', 'î', 'ì', 'í', 'ï', 'ı', 'ĵ', 'ł', 'ľ', 'ñ', 'ń', 'ň', 'ò', 'ö', 'ô', 'ó', 'õ', 'ø', 'ř', 'ŝ', 'ş', 'ś', 'š', 'ß', 'ť', 'þ', 'ù', 'ú', 'û', 'ŭ', 'ü', 'ů', 'ý', 'ź', 'ż', 'ž'};
  private String allowedString = new String(allowedletters);
  public void map(Text key, Text value, Context context)
    throws IOException, InterruptedException {

    String s = value.toString();

    // System.out.println(s);
    for (String letter : s.split("")) {
      // System.out.println(word + " | " + word.trim().length());
      if (allowedString.contains(letter)) {
        textPair.set(new Text(key), new Text(letter));
        context.write(textPair, new IntWritable(1));
        // Switch statement for language
        switch (key.toString()) {
          // french
          case "fr":
            context.getCounter(Counters.TOTAL_LETTERS_FR).increment(1);
            break;
          // German
          case "de":
            context.getCounter(Counters.TOTAL_LETTERS_DE).increment(1);
            break;
          // Spanish
          case "es":
            context.getCounter(Counters.TOTAL_LETTERS_ES).increment(1);
            break;
          // Italian
          case "it":
            context.getCounter(Counters.TOTAL_LETTERS_IT).increment(1);
            break;
          default:
            System.out.println("no match in Job1Mapper");
            break;
          }
      }
    }
  }
}
