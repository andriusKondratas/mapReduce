package com.office.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserClickFilterMapper extends Mapper<Object, Text, NullWritable, Text> {

    private String regexPattern = null;

    public void setup(Context context) {
        regexPattern = context.getConfiguration().get("countryRegex");
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] field = line.split(",", -1);
        String userId = field[1];
        if (userId.matches(regexPattern)) {
            context.write(NullWritable.get(), value);
        }
    }
}
