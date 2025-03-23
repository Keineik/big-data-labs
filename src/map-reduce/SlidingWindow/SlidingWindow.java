import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.NullWritable;

public class SlidingWindow {

  public static class WindowMapper
       extends Mapper<Object, Text, Text, DoubleWritable>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String[] record = value.toString().split(",");
      // If contains header
      if ("index".equals(record[0])) {
        // Add space character so that it will be on top in the sort and shuffle phase
        String header = " report_date,category,revenue";
        context.write(new Text(header), new DoubleWritable(0));
        return;
      }

      // If contains blank values
      if (record[15].trim().isEmpty()) return;

      // Get relevant columns
      LocalDate recordDate = LocalDate.parse(record[2], DateTimeFormatter.ofPattern("MM-dd-yy"));
      String category = record[9];
      double amount = Double.parseDouble(record[15]);

      for (int i = 0; i < 3; i++) {
        String outKey = recordDate.plusDays(i).toString() + "," + category;
        context.write(new Text(outKey), new DoubleWritable(amount));
      }
    }
  }

  public static class SumCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
      
      double sum = 0;
      for (DoubleWritable val : values) {
        sum += val.get();
      }
      
      context.write(key, new DoubleWritable(sum));
    }
  }

  public static class DoubleSumReducer
       extends Reducer<Text,DoubleWritable,Text,NullWritable> {

    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

      double sum = 0;
      for (DoubleWritable val : values) {
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

      String outKey = formattedDate + "," + parts[1] + "," + String.format("%.2f", sum);
      context.write(new Text(outKey), NullWritable.get());
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "sliding window");
    job.setJarByClass(SlidingWindow.class);
    job.setMapperClass(WindowMapper.class);
    job.setCombinerClass(SumCombiner.class);
    job.setReducerClass(DoubleSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}