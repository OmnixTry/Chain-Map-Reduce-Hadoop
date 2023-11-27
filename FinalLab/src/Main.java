import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Main {

    public static void main(String[] args) throws Exception {

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "dayDiff");

        job1.setJarByClass(Main.class);
        job1.setMapperClass(DayDiffMapper.class);
        job1.setReducerClass(DayDiffReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        // Delete output if exists
        Path outputDir = new Path(args[1]);
        FileSystem hdfs = FileSystem.get(conf1);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        job1.waitForCompletion(true);

        //
        // Job 2
        //

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "diffSum");
        job2.setJarByClass(Main.class);
        job2.setMapperClass(MonthGroupMapper.class);
        job2.setReducerClass(MonthSumReducer.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        // Delete output if exists
        outputDir = new Path(args[2]);
        hdfs = FileSystem.get(conf2);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }




    public static class DayDiffMapper
            extends Mapper<Object, Text, Text, FloatWritable>{
        public  void map(Object key, Text value, Context context) {

            String[] valueStr = value.toString().split(",");

            if(valueStr.length == 2) {
                try {
                    String currentDateStr = valueStr[0];
                    String currentTempStr = valueStr[1];

                    SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd");
                    Date date = parser.parse(currentDateStr);
                    float temperature = Float.parseFloat(currentTempStr);

                    context.write(new Text(currentDateStr), new FloatWritable(temperature));

                    Date tomorrowDate = addDays(date, 1);
                    context.write(new Text(parser.format(tomorrowDate)), new FloatWritable(-1 * temperature));

                }
                catch (Exception e) {
                }
            }
        }


        public static Date addDays(Date date, int days)
        {
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            cal.add(Calendar.DATE, days); //minus number would decrement the days
            return cal.getTime();
        }
    }

    public static class DayDiffReducer
            extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

            float sumValue = 0;
            int i = 0;
            for(FloatWritable val: values) {
                if(i == 0) {
                    sumValue = val.get();
                    i++;
                } else if (i == 1) {
                    sumValue += val.get();
                    i++;
                }
            }

            if(i == 2) {
                context.write(key, new FloatWritable(sumValue));
            }
        }
    }

    public static class MonthGroupMapper
            extends Mapper<Object, Text, IntWritable, IntWritable>{
        public  void map(Object key, Text value, Context context) {
            String[] valueStr = value.toString().split("\t");
            if(valueStr.length == 2) {
                try {
                    String dateStr = valueStr[0];
                    String tempStr = valueStr[1];

                    SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd");
                    Date date = parser.parse(dateStr);
                    float temperature = Float.parseFloat(tempStr);

                    int month = date.getMonth();

                    float thresholdValue = 0.18f;
                    if(temperature >= thresholdValue) {
                        context.write(new IntWritable(month), new IntWritable(1));
                    }
                    else if (temperature <= -1 * thresholdValue) {
                        context.write(new IntWritable(month), new IntWritable(-1));
                    }
                }
                catch (Exception e) {}
            }
        }
    }

    public static class MonthSumReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sumValue = 0;

            for(IntWritable val: values) {
                sumValue += val.get();
            }

            context.write(key, new IntWritable(sumValue));
        }
    }
}



/*

public class Main {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Main.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        Path outputDir = new Path(otherArgs[otherArgs.length - 1]);
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));

        // Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


 */