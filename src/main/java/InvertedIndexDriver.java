import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created with IntelliJ IDEA.
 * User: swetha
 * Date: 9/18/14
 * Time: 10:14 PM
 * To change this template use File | Settings | File Templates.
 */

/**
 * InvertedIndexDriver class: This is the main class that initiates calls to mappers, reducers and combiners.
 * Also the input, output formats are specified.
 *
 * Goal of the project:
 * For a given (Key) email address and its associated field (To, From, Cc, Bcc),
 * provide a (Value) list of Message IDs in which they appear.
 * Also, ensure that no key is duplicated and
 * the message Ids for a particular email address are unique
 *
 */
public class InvertedIndexDriver {

    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {

        //Configuration settings that specifies the configuration of the job
        Configuration conf = new Configuration();
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = new Job(conf, "InvertedIndexDriver");

        // Specify the JAR file to replicate to all machines.
        job.setJarByClass(InvertedIndexDriver.class);

        /**
         * Set the output key and value types.
         * Output Key: email_address (takes Text form)
         * Output value:
         *      a string of concatenated message_ids in which the email_address has occurred,
         *
         */

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the map, combiner and reduce classes.
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);

        // Set the input and output file formats.
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        // Grab the input file and output directory from the command line.
        FileInputFormat.addInputPath(job, new Path(appArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);

    }



}
