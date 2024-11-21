import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HighestVolumeDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: HighestVolumeDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Path outputPath = new Path(args[1]);

        // Check and delete the output path if it already exists
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println("Deleted existing output directory: " + args[1]);
        }

        Job job = Job.getInstance(conf, "Highest Trading Volume Analysis");
        job.setJarByClass(HighestVolumeDriver.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(HighestVolumeMapper.class);
        job.setReducerClass(HighestVolumeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Set output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);


        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        // Exit after job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
