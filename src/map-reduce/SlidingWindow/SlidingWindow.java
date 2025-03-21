import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.NullWritable;

public class SlidingWindow {

  public static class WindowMapper
       extends Mapper<Object, Text, Text, FloatWritable>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String[] record = value.toString().split(",");
      // If contains header
      if ("index".equals(record[0])) {
        // Add space character so that it will be on top in the sort and shuffle phase
        String header = " report_date,category,revenue";
        context.write(new Text(header), new FloatWritable(0));
        return;
      }

      // If contains blank values
      if (record[15].trim().isEmpty()) return;

      // Get relevant columns
      LocalDate recordDate = LocalDate.parse(record[2], DateTimeFormatter.ofPattern("MM-dd-yy"));
      String category = record[9];
      float amount = Float.parseFloat(record[15]);

      for (int i = 0; i < 3; i++) {
        String outKey = recordDate.plusDays(i).toString() + "," + category;
        context.write(new Text(outKey), new FloatWritable(amount));
      }
    }
  }

  public static class SumCombiner extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      
      float sum = 0;
      for (FloatWritable val : values) {
        sum += val.get();
      }
      
      context.write(key, new FloatWritable(sum));
    }
  }

  public static class FloatSumReducer
       extends Reducer<Text,FloatWritable,Text,NullWritable> {

    public void reduce(Text key, Iterable<FloatWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

      float sum = 0;
      for (FloatWritable val : values) {
        sum += val.get();
      }

      String[] parts = key.toString().split(",");
      // If is header
      if (" report_date".equals(parts[0])) {
        // Remove the space added in the map function
        String outKey = key.toString().substring(1);
        context.write(new Text(outKey), NullWritable.get());
        return;
      }

      // Format date
      String formattedDate = (LocalDate.parse(parts[0]))
                .format(DateTimeFormatter.ofPattern("dd/MM/yyyy"));

      // Format sum with two decimals
      String formattedSum = String.format("%.2f", sum);

      String outKey = formattedDate + "," + parts[1] + "," + formattedSum;
      context.write(new Text(outKey), NullWritable.get());
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "sliding window");
    job.setJarByClass(SlidingWindow.class);
    job.setMapperClass(WindowMapper.class);
    job.setCombinerClass(SumCombiner.class);
    job.setReducerClass(FloatSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}