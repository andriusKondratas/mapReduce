package com.office.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        var line = value.toString();
        var fields = line.split(",");
        context.write(new Text(fields[0]), new Text("country:" + fields[1]));
    }
}
