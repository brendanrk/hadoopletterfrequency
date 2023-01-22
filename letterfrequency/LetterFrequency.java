import org.apache.log4j.Logger;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The LetterFrequency class is the main driver for the hadoop jobs
 */
public class LetterFrequency extends Configured implements Tool{

  private final Logger log = Logger.getLogger(LetterFrequency.class.getName());

  /**
   * Job0 takes in the all the files from the input directory and 
   * process them to output the following format "<lang> <line>"
   * It gets the <lang> from the file path
   * the line has any spaces or punctuations removed 
   */
  private Job Job0(String inputText, String tempOuputPath) throws IOException {
    // Create Job
    Job job = Job.getInstance();
    job.setJarByClass(getClass());
    job.setJobName("Job0");

    Configuration mapConfig = new Configuration(false);
    // 1st mapper removes the characters/letters we don't want in the line
    // 1st mapper output: <line number>    <line string without punctuation> 
    // 1st mapper output e.g. 345    Crujir hace el viento
    ChainMapper.addMapper(job, Job0Mapper.class, LongWritable.class,
                                 Text.class, LongWritable.class, Text.class, mapConfig);

    // 2nd mapper adds the lang key, such as "es" "de" 
    // 2nd mapper output: <language>    <line string without punctuation> 
    // 1st mapper output e.g. es    Crujir hace el viento
    Configuration addLangMapConfig = new Configuration(false);
    ChainMapper.addMapper(job, Job0AddLangMapper.class, LongWritable.class,
                                 Text.class, Text.class, Text.class,
                                 addLangMapConfig);

    

    // configure input source
    FileInputFormat.setInputPaths(job, new Path(inputText));
    job.setInputFormatClass(TextInputFormat.class);

    // Setup Mapper
    // job.setMapperClass(Job0Mapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    //Set number of reducer tasks
    job.setNumReduceTasks(4); // 4 languages
    
    // Setup Partitioner
    // paritioner will separate stream into 4 based on the 4 languages
    job.setPartitionerClass(Job0Partitioner.class);

    // Set Output format
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileOutputFormat.setOutputPath(job, new Path(tempOuputPath));

    return job;
  }

  /**
   * Job1 takes in the all the files from the input directory and 
   * process them to output the following format "<lang> <letter> <count>"
   * The lines from the 1st job are split into letters and outputs them one by one
   * The reducer will then count them
   * The partiioner will divide the data based on the <lang> keys into 4 partitions
   * This job also uses a COUNTER to count the total letters seen in order to get percentages
   */
  private Job Job1(String inputText, String tempOuputPath) throws IOException {
    // Create Job
    Job job = Job.getInstance();
    job.setJarByClass(getClass());
    job.setJobName("Job1");

    // configure input source
    FileInputFormat.setInputPaths(job, new Path(inputText));
    job.setInputFormatClass(KeyValueTextInputFormat.class);

    // Setup Mapper
    job.setMapperClass(Job1Mapper.class);
    job.setMapOutputKeyClass(TextPair.class);
    job.setMapOutputValueClass(IntWritable.class);

    //Set number of reducer tasks
    job.setNumReduceTasks(4); // 4 languages
    
    // // Setup Partitioner
    job.setPartitionerClass(Job1Partitioner.class);

    // Setup Combiner
    job.setCombinerClass(Job1Combiner.class);

    // Setup Reducer
    job.setReducerClass(Job1Reducer.class);

    // Set Output format
    job.setOutputKeyClass(TextPair.class);
    job.setOutputValueClass(Text.class);

    FileOutputFormat.setOutputPath(job, new Path(tempOuputPath));

    return job;
  }

  /**
   * this is entry point to run the hadoop jobs 
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new LetterFrequency(),
    args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: LetterFrequency <input path> <output path>");
      System.exit(-1);
    }
    System.out.println(" In Driver now!");

    // Need to create a temp directory to allow the data to be input to the 2nd job
    String intermediateTempDir = args[1] + "-" + getClass().getSimpleName() + "-tmp";
    System.out.println("intermediateTempDir: " + intermediateTempDir);

    // create jobs to split the task up into
    // job0: prepare lines and add language key
    // job1: split the lines into letters and count them while partitioning the data into 4 groups
    JobControl control = new JobControl("Workflow-Example");
    ControlledJob step1 = new ControlledJob(Job0(args[0], intermediateTempDir), null);
    ControlledJob step2 = new ControlledJob(Job1(intermediateTempDir, args[1]), null);

    step2.addDependingJob(step1);
    control.addJob(step1);
    control.addJob(step2);


    Thread workflowThread = new Thread(control,"Workflow-Thread");
    workflowThread.setDaemon(true);
    workflowThread.start();

    // Monitor the output of the jobs and print the failure to the log output for each job
    while (!control.allFinished()){
      Thread.sleep(10);
    }
    if ( control.getFailedJobList().size() > 0 ){
      log.error(control.getFailedJobList().size() + " jobs failed!");
      for ( ControlledJob job : control.getFailedJobList()){
        log.error(job.getJobName() + " failed");
      }
    } else {
      log.info("Success!! Workflow completed [" + control.getSuccessfulJobList().size() + "] jobs");
    }

    return 0;
  }
}
