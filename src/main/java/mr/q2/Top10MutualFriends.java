package mr.q2;

import com.google.common.collect.Ordering;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
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

import java.io.IOException;
import java.util.*;

public class Top10MutualFriends {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        String[] files = new GenericOptionsParser(configuration, args).getRemainingArgs();

        Path output = new Path(files[1]);
        Path input = new Path(files[0]);

        Job j = Job.getInstance(configuration, "top-10-mutual-list");
        j.setJarByClass(Top10MutualFriends.class);
        j.setReducerClass(ReducerClass.class);
        j.setMapperClass(MapperClass.class);

        /*
        You can set the number of reducers to 10, but you would need another mr job to consolidate their output.
        j.setNumReduceTasks(10);
        */

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(Text.class);

        j.setOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            int tabChar = line.indexOf('\t');
            if(tabChar != -1) {
                String userId = line.substring(0, tabChar);
                String values = line.substring(tabChar + 1);
                writeContent(userId, values, con);
            }
        }

        static void writeContent(String key, String values, Context con) throws IOException, InterruptedException {
            int user1 = Integer.parseInt(key);
            if (!values.contains(",")) return;
            String[] val = values.split(",");

            for (String s : val) {
                int id = Integer.parseInt(s);
                if (user1 == id) continue;

                StringBuilder sb = new StringBuilder();
                if (user1 > id) {
                    sb.append(user1);
                    sb.append("_");
                    sb.append(s);
                } else {
                    sb.append(s);
                    sb.append("_");
                    sb.append(user1);
                }

                con.write(new Text(sb.toString()), new Text(values));
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, IntWritable> {

        public static class KeyAndValues {

            String key;
            String values;

            private KeyAndValues(String key, String values){
                this.key = key;
                this.values = values;
            }

            @Override
            public String toString(){
                return key + " : " + values + " -> ";
            }

            private int compare(KeyAndValues obj){
                return key.compareTo(obj.key);
            }
        }


        private SortedSetMultimap<Integer, KeyAndValues> treeMap = TreeMultimap.create(Ordering.natural().reverse(),
                KeyAndValues::compare);

        public void reduce(Text word, Iterable<Text> values, Context con) {
            Map<Integer, Integer> map = new HashMap<>();

            for (Text value : values) {
                String[] vals = value.toString().split(",");

                for (String i : vals) {
                    if(i.equals("")) continue;
                    int val = Integer.parseInt(i);
                    if (!map.containsKey(val))
                        map.put(val, 1);
                    else
                        map.put(val, map.get(val) + 1);
                }
            }

            int c = 0;
            StringBuilder sb = new StringBuilder();
            String pre = "";
            for (Map.Entry<Integer, Integer> e : map.entrySet()) {
                if (e.getValue() == 2) {
                    c++;
                    sb.append(pre);
                    pre = ",";
                    sb.append(e.getKey());
                }
            }

            if(c > 0) {

                KeyAndValues temp = new KeyAndValues(word.toString(), sb.toString());

                if (treeMap.size() < 10)
                    treeMap.put(c, temp);
                else {
                    Integer removeKey = 0;
                    KeyAndValues removeValue = null;

                    for (Map.Entry<Integer, KeyAndValues> a: treeMap.entries()){
                        removeKey = a.getKey();
                        removeValue = a.getValue();
                    }

                    if (removeValue != null && removeKey < c) {
                        treeMap.remove(removeKey, removeValue);
                        treeMap.put(c, temp);
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Integer e : treeMap.keySet())
                for (KeyAndValues v: treeMap.get(e))
                    context.write(new Text(v.toString()), new IntWritable(e));
        }
    }

}