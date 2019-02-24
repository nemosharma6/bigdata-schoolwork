package mr.q3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class JoinInMemory {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        String[] files = new GenericOptionsParser(configuration, args).getRemainingArgs();

        String user1 = files[0];
        String user2 = files[1];
        String userFile = files[2];
        Path input = new Path(files[3]);
        Path output = new Path(files[4]);

        configuration.set("userFile", userFile);
        configuration.set("user1", user1);
        configuration.set("user2", user2);

        Job j = Job.getInstance(configuration, "in-memory-join");
        j.setJarByClass(JoinInMemory.class);
        j.setReducerClass(JoinInMemory.ReducerClass.class);
        j.setMapperClass(JoinInMemory.MapperClass.class);
        j.setOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);

        FileOutputFormat.setOutputPath(j, output);
        FileInputFormat.addInputPath(j, input);
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            Configuration conf = con.getConfiguration();
            String user1 = conf.get("user1");
            String user2 = conf.get("user2");

            String line = value.toString();

            int tabChar = line.indexOf('\t');
            if (tabChar != -1) {
                String userId = line.substring(0, tabChar);
                if (!userId.equals(user1) && !userId.equals(user2)) return;

                String[] values = line.substring(tabChar + 1).split(",");
                for (String val : values) {
                    Text outputKey = new Text(user1 + "_" + user2);
                    IntWritable outputValue = new IntWritable(Integer.parseInt(val));
                    con.write(outputKey, outputValue);
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, IntWritable, Text, Text> {

        Map<Integer, String> address = new HashMap<>();
        Map<Integer, String> name = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {

            Configuration conf = context.getConfiguration();
            String userFile = conf.get("userFile");
            Path uFile = new Path(userFile);

            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(uFile)));
            String line = br.readLine();
            while (line != null) {
                String[] words = line.split(",");
                int id = Integer.parseInt(words[0]);
                String city = words[4];
                address.put(id, city);
                name.put(id, words[1]);
                line = br.readLine();
            }
        }

        public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
            Map<Integer, Integer> mp = new HashMap<>();

            for (IntWritable value : values) {
                if (mp.containsKey(value.get())) mp.put(value.get(), mp.get(value.get()) + 1);
                else mp.put(value.get(), 1);
            }

            ArrayList<String> ad = new ArrayList<>();
            ArrayList<String> nm = new ArrayList<>();
            for (Map.Entry<Integer, Integer> e : mp.entrySet()) {
                if (e.getValue() == 2) {
                    ad.add(address.get(e.getKey()));
                    nm.add(name.get(e.getKey()));
                }
            }

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("[");
            String pre = "";
            for (int i = 0; i < nm.size(); i++) {
                stringBuilder.append(pre);
                pre = ",";
                stringBuilder.append(nm.get(i));
                stringBuilder.append(":");
                stringBuilder.append(ad.get(i));
            }

            stringBuilder.append("]");
            con.write(word, new Text(stringBuilder.toString()));
        }
    }

}
