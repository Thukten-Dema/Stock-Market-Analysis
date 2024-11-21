import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HighestVolumeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text contentId = new Text(); // Reusable key object
    private DoubleWritable engagement = new DoubleWritable(); // Reusable value object

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header row
        if (key.get() == 0 && value.toString().toLowerCase().contains("date")) {
            return;
        }

        // Split the input line by commas
        String[] fields = value.toString().split(",");

        // Check for the expected number of fields (6 in this case)
        if (fields.length == 6) {
            try {
                // Extract and clean the relevant fields
                String date = fields[0].trim();
                String volumeStr = fields[5].trim();

                // Validate the volume field
                if (!volumeStr.isEmpty()) {
                    double volume = Double.parseDouble(volumeStr);

                    // Set key and value
                    contentId.set(date);
                    engagement.set(volume);

                    // Emit the key-value pair
                    context.write(contentId, engagement);
                }
            } catch (NumberFormatException e) {
                // Log numeric parsing errors and continue
                System.err.println("Skipping row due to invalid number format: " + value.toString());
            }
        } else {
            // Log malformed rows
            System.err.println("Skipping malformed row: " + value.toString());
        }
    }
}
