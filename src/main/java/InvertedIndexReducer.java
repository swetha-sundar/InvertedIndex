import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * Created with IntelliJ IDEA.
 * User: swetha
 * Date: 9/18/14
 * Time: 10:15 PM
 * To change this template use File | Settings | File Templates.
 */

/**
 *  Reads combined output of the combiners and aggregates values of common keys
 *  Input:
 *      Key= Header_type:Email_address
 *      Value= Concatenated list of Message_Ids corresponding to key
 *  Output:
 *      Key= Header_type:Email_address
 *      Value= Complete list of Message_Ids corresponding to that email_address
 */

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

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
        ArrayList<String> outputValues = new ArrayList<String>(messageIds.size());
        outputValues.addAll(messageIds);

        //Calling a Utility function combine so as to concatenate every element in an arrayList with a delimiter
        messageIdList = new Text (WritableUtils.combine(outputValues,delimiter));

        //Writing the key and output value
        context.write(key, messageIdList);

    }
}
