package mr.q4;

import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class JobChaining extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new JobChaining(), args);
        System.exit(ret);
    }

    public int run(String[] args) throws Exception {
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        JobControl jobControl = new JobControl("chain");

        String userFile = files[0];
        Path input = new Path(files[1]);
        Path tempOutput = new Path(files[2]);
        Path output = new Path(files[3]);
        c.set("user", userFile);

        Job j1 = Job.getInstance(c, "job-chaining");
        j1.setJarByClass(JobChaining.class);
        j1.setMapperClass(JobChaining.MapperClass1.class);
        j1.setReducerClass(JobChaining.ReducerClass1.class);

        j1.setOutputValueClass(IntWritable.class);
        j1.setOutputKeyClass(IntWritable.class);

        FileOutputFormat.setOutputPath(j1, tempOutput);
        FileInputFormat.addInputPath(j1, input);

        ControlledJob controlledJob1 = new ControlledJob(c);
        controlledJob1.setJob(j1);
        jobControl.addJob(controlledJob1);

        Job j2 = Job.getInstance(c, "bottom-15");
        j2.setJarByClass(JobChaining.class);
        j2.setMapperClass(JobChaining.SecondMapperClass.class);

        j2.setOutputKeyClass(IntWritable.class);
        j2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j2, tempOutput);
        FileOutputFormat.setOutputPath(j2, output);

        ControlledJob controlledJob2 = new ControlledJob(c);
        controlledJob2.setJob(j2);

        controlledJob2.addDependingJob(controlledJob1);
        jobControl.addJob(controlledJob2);

        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();

        while (!jobControl.allFinished()) {
            System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());
            System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
            System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
            System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
            System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
            System.out.println("----------------------------------------");

            try {
                Thread.sleep(5000);
            } catch (Exception e) {

            }
        }

        System.exit(0);

        return 0;
    }

    public static class MapperClass1 extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        Map<Integer, String> age = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {

            Configuration conf = context.getConfiguration();
            String user = conf.get("user");
            Path userFile = new Path(user);

            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(userFile)));
            String line;

            while ((line = br.readLine()) != null) {
                String[] words = line.split(",");
                int id = Integer.parseInt(words[0]);
                String dob = words[9];
                age.put(id, dob);
            }
        }

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            int tabChar = line.indexOf('\t');

            int dur = 0;
            String userId = line.substring(0, tabChar);
            if(!line.contains(",")) return;
            String[] values = line.substring(tabChar + 1).split(",");

            for (String val : values) {
                if(val.equals("")) continue;
                String dob = age.get(Integer.parseInt(val));
                DateFormat format = new SimpleDateFormat("MM/dd/yyyy");
                Date date;

                try {
                    date = format.parse(dob);
                } catch (Exception e) {
                    continue;
                }

                int timeBetween = (int) Math.floor((new Date().getTime() - date.getTime()) / 3.15576e+10);
                dur += timeBetween;
            }

            dur = dur / values.length;
            IntWritable outputKey = new IntWritable(Integer.parseInt(userId));
            IntWritable outputValue = new IntWritable(dur);

            /* Looking for age > 0 */
            if(dur > 0) con.write(outputValue, outputKey);
        }
    }

    public static class ReducerClass1 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public void reduce(IntWritable age, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
            for (IntWritable i : values)
                con.write(i, age);
        }
    }

    public static class SecondMapperClass extends Mapper<LongWritable, Text, IntWritable, Text> {

        Map<Integer, String> address = new HashMap<>();
        private SortedSetMultimap<Integer, Integer> treeMap = TreeMultimap.create();

        @Override
        protected void setup(Context context) throws IOException {

            Configuration conf = context.getConfiguration();
            String user = conf.get("user");
            Path userFile = new Path(user);

            String line;
            FileSystem f = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(f.open(userFile)));

            while ((line = br.readLine()) != null) {
                String[] words = line.split(",");
                int id = Integer.parseInt(words[0]);
                String add = words[3] + "," + words[4] + "," + words[5];
                address.put(id, add);
            }
        }

        public void map(LongWritable key, Text value, Context con) {
            String line = value.toString();
            int tabChar = line.indexOf('\t');

            Integer userId = Integer.parseInt(line.substring(0, tabChar));
            Integer age = Integer.parseInt(line.substring(tabChar + 1));

            treeMap.put(age, userId);
            if (treeMap.size() > 15) {
                Integer removeKey = null;
                Integer removeValue = null;

                for (Map.Entry<Integer, Integer> a : treeMap.entries()) {
                    removeKey = a.getKey();
                    removeValue = a.getValue();
                }

                treeMap.remove(removeKey, removeValue);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            for (Map.Entry<Integer, Integer> a : treeMap.entries()) {
                Integer age = a.getKey();
                Integer id = a.getValue();
                String add = address.get(id);
                context.write(new IntWritable(id), new Text(add + " " + age.toString()));
            }
        }
    }
}
