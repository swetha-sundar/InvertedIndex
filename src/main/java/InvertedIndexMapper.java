import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * Created with IntelliJ IDEA.
 * User: swetha
 * Date: 9/18/14
 * Time: 10:15 PM
 * To change this template use File | Settings | File Templates.
 */
public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    //Variables to store the Key and Value of the Mapper
    private Text emailAddress;
    private Text messageId;

    //Variables that store the pre-defined email headers
    private static final String msgID = "Message-ID:";
    private static final String from = "From:";
    private static final String to = "To:";
    private static final String cc = "Cc:";
    private static final String bcc = "Bcc:";
    private static final String stopParserTerm = "X-From:";

    //Delimiter that specifies how email addresses are separated from one another in each field
    private static final String delimiter = ",";

    /**
     * Map function:
     *  Input: reads every email (a single line is one email)
     *          Key: a line offset
     *          Value: one entire email
     *  Output: emits every email_address along with its message_id
     *          Key: email_header_type concatenated with the email_address.
     *          Header refers to "To:, From:, Bcc: Cc:"
     *          Value: the message_id in which the email_address appeared
     *
     */

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String emailToBeParsed = value.toString();

        //Chunk the email into tokens where each token is delimited by a space
        StringTokenizer tokenizer = new StringTokenizer(emailToBeParsed);

        /**
         * ArrayList: A data structure used to store the list of email addresses that are
         * embedded in each header field of the email.
         */

        ArrayList<String> fields = new ArrayList<String>();

        //Iterate through every token in the email to obtain the message Id and the email addresses
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();

            //Stop parsing when you see the stopParserTerm which is "X-From:"
            if (token.startsWith(stopParserTerm)) {
                break;
            }

            /**
             * If you encounter a token that starts with the field type "Message-ID:",
             * then obtain the next token which is the actual message_id
             * Else if you encounter other fields of interest, obtain every email_address
             * that it encloses
             */

            if (token.startsWith(msgID)) {
                messageId = new Text(tokenizer.nextToken());
            }
            else if (token.startsWith(from) || token.startsWith(to) || token.startsWith(cc) || token.startsWith(bcc)) {
                //Extract the field type so as to concatenate it along with every key
                String fieldType = token;
                token = tokenizer.nextToken();
                fields.add(fieldType.concat(token));

                //Iterate further through the email to find the other consecutive email_addr in the same field.
                //These are delimited by a comma and so
                //continue to parse as long as you see a comma separated email_addr

                while (tokenizer.hasMoreTokens() && token.endsWith(delimiter)) {
                    token = tokenizer.nextToken();
                    fields.add(fieldType.concat(token));
                }
            }
        }

        //Iterate through the stored ArrayList to emit the key and value from the mapper.

        for (int i = 0; i < fields.size(); i++) {
            String field = fields.get(i);

            //Removing the trailing commas in the key field
            field = field.replaceAll(",$","");

            emailAddress = new Text(field);

            //Writing the key and value for every email address encountered
            context.write(emailAddress,messageId);
        }
    }
}
