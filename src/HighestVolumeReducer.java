import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HighestVolumeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private Text maxVolumeDate = new Text();
    private double maxVolume = 0;

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        for (DoubleWritable value : values) {
            if (value.get() > maxVolume) {
                maxVolume = value.get();
                maxVolumeDate.set(key);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Emit the date with the highest volume and its value as a single output
        context.write(new Text("Date with Highest Volume: " + maxVolumeDate.toString()), new DoubleWritable(maxVolume));
    }
}
