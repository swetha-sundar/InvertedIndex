import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeSet;

/**
 * Created with IntelliJ IDEA.
 * User: swetha
 * Date: 9/20/14
 * Time: 2:58 PM
 * To change this template use File | Settings | File Templates.
 */

/**
 *  Reads outputs of mappers and combines values of common keys
 *  Input:
 *      Key= Header_type:Email_address
 *      Value= Iterable list of Message_Ids corresponding to key
 *  Output:
 *      Key= Header_type:Email_address
 *      Value= Concatenated Message_Ids
 */

public class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {

    private static final String delimiter = ",";
    private Text messageIdList;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        //TreeSet messageIds: A data structure used to store the list of message Ids.
        //Since it is a set, only unique Ids are stored and
        //secondly, the Ids are stored in a sorted fashion

        TreeSet<String> messageIds = new TreeSet<String>();

        //Iterate through the values and add every single message Id to the TreeSet object
        for (Text value : values) {
            String messageId = value.toString();
            messageIds.add(messageId);
        }

        //ArrayList outputValues: A data structure used to store the message Ids in an array fashion
        //making it easier for concatenation purposes
        ArrayList<String> outputValues = new ArrayList<String>(messageIds);

        //Calling a Utility function combine so as to concatenate every element in an arraylist with a delimiter
        messageIdList = new Text (WritableUtils.combine(outputValues,delimiter));

        //Writing the key and output value
        context.write(key, messageIdList);
    }
}
