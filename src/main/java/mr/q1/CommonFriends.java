package mr.q1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
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

public class CommonFriends {
    public static void main(String[] args) throws Exception {
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job j = Job.getInstance(c, "common-friend-list");
        j.setJarByClass(CommonFriends.class);
        j.setMapperClass(MapperClass.class);
        j.setReducerClass(ReducerClass.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            int tabChar = line.indexOf('\t');
            String userId = line.substring(0, line.indexOf('\t'));
            String[] values = line.substring(tabChar + 1).split(",");
            int id = Integer.parseInt(userId);

            if (id == 0)
                writeContent(userId + "_1", values, con);
            else if (id == 1) {
                writeContent("0_" + userId, values, con);
                writeContent(userId + "_29826", values, con);
            } else if (id == 20)
                writeContent(userId + "_28193", values, con);
            else if (id == 28193)
                writeContent("20_" + userId, values, con);
            else if (id == 29826)
                writeContent("1_" + userId, values, con);
            else if (id == 6222)
                writeContent(userId + "_19272", values, con);
            else if (id == 19272)
                writeContent("6222_" + userId, values, con);
            else if (id == 28041)
                writeContent(userId + "_28056", values, con);
            else if (id == 28056)
                writeContent("28041_" + userId, values, con);

        }

        static void writeContent(String key, String[] values, Context con) throws IOException, InterruptedException {
            for (String val : values) {
                Text outputKey = new Text(key);
                IntWritable outputValue = new IntWritable(Integer.parseInt(val));
                con.write(outputKey, outputValue);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
            Map<Integer, Integer> map = new HashMap<>();

            for (IntWritable value : values) {
                int val = value.get();
                if (map.containsKey(val))
                    map.put(val, map.get(val) + 1);
                else
                    map.put(val, 1);
            }

            ArrayList<Integer> sol = new ArrayList<>();
            for (Map.Entry<Integer, Integer> e : map.entrySet()) {
                if (e.getValue() == 2)
                    sol.add(e.getKey());
            }

            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < sol.size() - 1; i++) {
                stringBuilder.append(sol.get(i));
                stringBuilder.append(",");
            }

            if (sol.size() >= 1)
                stringBuilder.append(sol.get(sol.size() - 1));
            con.write(word, new Text(stringBuilder.toString()));
        }
    }
}