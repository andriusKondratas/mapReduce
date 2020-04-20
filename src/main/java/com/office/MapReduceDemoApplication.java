package com.office;


import com.office.mapper.ClickMapper;
import com.office.reducer.SumReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapReduceDemoApplication {

    public static void main(String[] args) throws Exception {
        System.out.printf("Homework is fun game !%n");

        Configuration conf = new Configuration();
        Job job = setUpJob(args[0], conf);
        job.setJarByClass(MapReduceDemoApplication.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static Job setUpJob(String jobName, Configuration conf) throws Exception {
        Job job = null;
        switch (jobName) {
            case "ClickCount": {
                job = Job.getInstance(conf, "user click count");
                job.setMapperClass(ClickMapper.class);
                job.setCombinerClass(SumReducer.class);
                job.setReducerClass(SumReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                System.out.printf("Will execute %s case%n", "ClickCount");
                break;
            }
            default: {
                System.out.printf("Oops! I don't know about job %s%n", jobName);
                System.out.printf("Possible options: %s,%s%n", "WordCount", "ClickCount");
                System.exit(1);
            }
        }
        return job;
    }

}

