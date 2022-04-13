import java.io.IOException;
import java.util.PriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jetbrains.annotations.NotNull;

/**
 * part B
 */
public class Top10Service {

    public static class TsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            InputSplit inputSplit =context.getInputSplit();
            String filePath = ((FileSplit)inputSplit).getPath().toString();
            String[] tmp = value.toString().split(",");
            if (filePath.contains("contract")) {
                if (!tmp[0].equals("address")) {
                    context.write(new Text(tmp[0]), new Text("c"));
                }
            } else {
                if (!tmp[0].equals("block_number")) {
                    context.write(new Text(tmp[2]), new Text("t"));
                }
            }
        }
    }

    public static class TsReducer extends Reducer<Text, Text, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int cnt = 0;
            boolean inContract = false;
            for (Text val : values) {
                if (val.toString().equals("c")) {
                    inContract = true;
                }
                cnt++;
            }

            if (inContract) {
                context.write(key, new IntWritable(cnt));
            }
        }
    }

    public static class SortMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tmp = value.toString().split("\t");
            context.write(new Text(tmp[0]), new IntWritable(Integer.parseInt(tmp[1])));
        }
    }

    public static class SortReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        PriorityQueue<KV> kvList;

        @Override
        protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            kvList = new PriorityQueue<>();
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable val : values) {
                kvList.add(new KV(key.toString(), val.get()));
                if (kvList.size() > 10) {
                    kvList.poll();
                }
            }
        }

        @Override
        protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            for (KV kv: kvList) {
                context.write(new Text(kv.key), new IntWritable(kv.val));
            }
        }

        private class KV implements Comparable<KV> {
            String key;
            int val;

            public KV(String key, int val) {
                this.key = key;
                this.val = val;
            }

            @Override
            public int compareTo(@NotNull Top10Service.SortReducer.KV o) {
                return Integer.compare(o.val, val);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        String transaction = args[0];
        String contract = args[1];
        String output = args[2];

        Job job = Job.getInstance(conf, "Aggregate");
        job.setJarByClass(Top10Service.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(TsMapper.class);
        job.setReducerClass(TsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(transaction));
        FileInputFormat.addInputPath(job, new Path(contract));
        Path tmpOutputPath = new Path(output + "_tmp");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(tmpOutputPath)) {
            fs.delete(tmpOutputPath, true);
        }

        FileOutputFormat.setOutputPath(job, tmpOutputPath);

        if (job.waitForCompletion(true)) {
            job = Job.getInstance(conf, "Sort");
            job.setJarByClass(Top10Service.class);
            job.setNumReduceTasks(1);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setMapperClass(SortMapper.class);
            job.setReducerClass(SortReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setInputFormatClass(TextInputFormat.class);

            FileInputFormat.addInputPath(job, tmpOutputPath);
            Path outputPath = new Path(output);
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }

            FileOutputFormat.setOutputPath(job, outputPath);
            job.waitForCompletion(true);
        }
    }
}
