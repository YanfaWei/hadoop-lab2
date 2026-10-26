import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {
    public static class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Set<String> stop = new HashSet<>();
        private final Text wordOut = new Text();
        private final Text fileOut = new Text();

        @Override
        protected void setup(Context context) throws IOException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            try (BufferedReader br = new BufferedReader(new InputStreamReader(
                    fs.open(new Path("/user/weiyifan/lab2/stopwords.txt"))))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim().toLowerCase();
                    if (!line.isEmpty()) stop.add(line);
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            fileOut.set(fileName);
            String[] words = value.toString().toLowerCase().replaceAll("[^a-z]", " ").split("\\s+");
            for (String word : words) {
                if (word.length() < 2 || stop.contains(word)) continue;
                wordOut.set(word);
                context.write(wordOut, fileOut);
            }
        }
    }

    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TreeMap<String, Integer> freq = new TreeMap<>();
            for (Text value : values) {
                String name = value.toString();
                freq.put(name, freq.getOrDefault(name, 0) + 1);
            }
            StringBuilder sb = new StringBuilder();
            int total = 0;
            for (var entry : freq.entrySet()) {
                sb.append("<").append(entry.getKey()).append(",").append(entry.getValue()).append(">;");
                total += entry.getValue();
            }
            sb.append("<total,").append(total).append(">.");
            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        Job job = Job.getInstance(conf, "inverted-index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
