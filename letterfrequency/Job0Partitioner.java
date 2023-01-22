import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Job0Partitioner: use the <lang> key to divide the data into 
 * partitions
 */
public class Job0Partitioner extends Partitioner<Text, Text> {
    private final Logger log = Logger.getLogger(getClass().getName());

    public int getPartition(Text key, Text value, int numReduceTasks) {
        String s = key.toString();
        s = s.substring(0, 2);
        log.info(s);
        log.info(numReduceTasks);
        return (s.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}