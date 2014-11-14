import org.apache.hadoop.util.StringUtils;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: swetha
 * Date: 9/20/14
 * Time: 3:46 PM
 * To change this template use File | Settings | File Templates.
 */

//A utility class
public class WritableUtils {

    //A utility function that joins every single element in a given arrayList with a delimiter
    public static String combine(ArrayList<String> arr, String delimiter) {
        return (StringUtils.join(delimiter,arr));
    }

}
