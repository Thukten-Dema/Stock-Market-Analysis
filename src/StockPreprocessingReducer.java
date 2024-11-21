import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockPreprocessingReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private int totalNullCount = 0; // Initialize a total null count for all columns

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int nullCount = 0; // Initialize null count for each column

        // Sum up the null counts for each column
        for (IntWritable count : values) {
            nullCount += count.get();
        }

        totalNullCount += nullCount; // Update the total null count

        // Output the column and its total null count
        context.write(key, new IntWritable(nullCount));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Check if total null count is zero
        if (totalNullCount == 0) {
            context.write(new Text("No null values found in data"), new IntWritable(0)); // Output message
        }
    }
}