package com.office;


import com.office.mapper.ClickMapper;
import com.office.mapper.UserClickMapper;
import com.office.mapper.UserMapper;
import com.office.reducer.SumReducer;
import com.office.reducer.UserClickReducer;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;


public class MapReduceDemoApplication {

    public static void main(String[] args) throws Exception {
        System.out.printf("Start map reduce n");

        var job = setUpJob(args);
        job.setJarByClass(MapReduceDemoApplication.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static Job setUpJob(String args[]) throws Exception {
        Job job = null;
        var conf = new Configuration();
        switch (args[0]) {
            case "ClickCount": {
                job = Job.getInstance(conf, "user click count");
                job.setMapperClass(ClickMapper.class);
                job.setCombinerClass(SumReducer.class);
                job.setReducerClass(SumReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                FileInputFormat.addInputPath(job, new Path(args[1]));
                FileOutputFormat.setOutputPath(job, new Path(args[2]));
                FileUtils.deleteDirectory(new File(args[2]));
                System.out.printf("Will execute %s case%n", "ClickCount");
                break;
            }
            case "ClickFilter": {
                conf.set("countryRegex", args[1]);
                job = Job.getInstance(conf, "user click filter");
                job.setReducerClass(UserClickReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, UserMapper.class);
                MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, UserClickMapper.class);
                FileOutputFormat.setOutputPath(job, new Path(args[4]));
                FileUtils.deleteDirectory(new File(args[4]));
                System.out.printf("Will execute %s case%n", "ClickFilter");
                break;
            }
            default: {
                System.out.printf("Oops! I don't know about job %s%n", args[0]);
                System.out.printf("Possible options: %s,%s%n", "WordCount", "ClickCount");
                System.exit(1);
            }
        }
        return job;
    }
}

