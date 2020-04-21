package com.office.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class UserClickReducer extends Reducer<Text, Text, Text, Text> {

    private String regexPattern = null;

    public void setup(Context context) {
        regexPattern = context.getConfiguration().get("countryRegex");
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
            InterruptedException {
        if (regexPattern == null) {
            regexPattern = context.getConfiguration().get("countryRegex");
        }

        var match = false;
        var output = new ArrayList<String>();
        for (var value : values) {
            var groups = value.toString().split(":");
            if (groups[0].equals("click")) {
                output.add(groups[1]);
            } else {
                if (groups[1].matches(regexPattern)) {
                    match = true;
                }
            }
        }
        if (match) {
            for (var out : output) {
                var columns = out.split(",");
                context.write(new Text(columns[0]), new Text(columns[1] + " " + columns[2]));
            }
        }
    }
}
