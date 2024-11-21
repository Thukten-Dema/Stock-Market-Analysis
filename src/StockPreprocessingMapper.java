
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StockPreprocessingMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    private final IntWritable one = new IntWritable(1); // Constant for null count increment
    private Text columnKey = new Text(); // Reusable key for each column

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header line
        if (key.get() == 0 && value.toString().contains("Date")) {
            return;
        }

        // Split the row into columns
        String[] fields = value.toString().split(",");

        // Check for null values in each field and emit a count if null
        if (fields.length >= 6) {
            if (fields[0].trim().isEmpty()) { // Check "Date" column
                columnKey.set("Date");
                context.write(columnKey, one);
            }
            if (fields[1].trim().isEmpty()) { // Check "Open Price" column
                columnKey.set("OpenPrice");
                context.write(columnKey, one);
            }
            if (fields[2].trim().isEmpty()) { // Check "High Price" column
                columnKey.set("HighPrice");
                context.write(columnKey, one);
            }
            if (fields[3].trim().isEmpty()) { // Check "Low Price" column
                columnKey.set("LowPrice");
                context.write(columnKey, one);
            }
            if (fields[4].trim().isEmpty()) { // Check "Close Price" column
                columnKey.set("ClosePrice");
                context.write(columnKey, one);
            }
            if (fields[5].trim().isEmpty()) { // Check "Volume" column
                columnKey.set("Volume");
                context.write(columnKey, one);
            }
        }
    }
}