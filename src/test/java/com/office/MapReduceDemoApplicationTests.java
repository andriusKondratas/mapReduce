package com.office;


import com.office.mapper.ClickMapper;
import com.office.mapper.UserClickMapper;
import com.office.mapper.UserMapper;
import com.office.reducer.SumReducer;
import com.office.reducer.UserClickReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;


class MapReduceDemoApplicationTests {

    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    MapDriver<Object, Text, Text, Text> mapJoinDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    ReduceDriver<Text, Text, Text, Text> reduceJoinDriver;
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
    MultipleInputsMapReduceDriver<Text, Text, Text, Text> mapReduceJoinDriver;

    @BeforeEach
    public void setUp() {
        mapDriver = new MapDriver<>();
        mapJoinDriver = new MapDriver<>();
        reduceDriver = new ReduceDriver<>();
        reduceJoinDriver = new ReduceDriver<>();
        mapReduceDriver = new MapReduceDriver<>();
        mapReduceJoinDriver = new MultipleInputsMapReduceDriver<>();
    }

    @Test
    public void testClickMapper() throws IOException {
        //setup
        var mapper = new ClickMapper();
        mapDriver.setMapper(mapper);

        mapDriver.withInput(new LongWritable(1), new Text("2020.01.01"))
                .withInput(new LongWritable(1), new Text("2020.01.01"))
                .withInput(new LongWritable(1), new Text("2020.01.02"))

                .withOutput(new Text("2020.01.01"), new IntWritable(1))
                .withOutput(new Text("2020.01.01"), new IntWritable(1))
                .withOutput(new Text("2020.01.02"), new IntWritable(1))

                //assert
                .runTest();
    }

    @Test
    public void testUserClickMapper() throws IOException {
        //setup
        var mapper = new UserClickMapper();
        mapJoinDriver.setMapper(mapper);

        mapJoinDriver.withInput(new LongWritable(1), new Text("2020.01.01,1,/profile/view"))
                .withInput(new LongWritable(1), new Text("2020.01.01,2,/items/search"))
                .withInput(new LongWritable(1), new Text("2020.01.02,3,/item/add"))
                .withInput(new LongWritable(1), new Text("2020.01.01,1,/item/delete"))

                .withOutput(new Text("1"), new Text("click:2020.01.01,1,/profile/view"))
                .withOutput(new Text("2"), new Text("click:2020.01.01,2,/items/search"))
                .withOutput(new Text("3"), new Text("click:2020.01.02,3,/item/add"))
                .withOutput(new Text("1"), new Text("click:2020.01.01,1,/item/delete"))

                //assert
                .runTest();
    }

    @Test
    public void testUserMapper() throws IOException {
        //setup
        var mapper = new UserMapper();
        mapJoinDriver.setMapper(mapper);

        mapJoinDriver.withInput(new LongWritable(1), new Text("1,LT"))
                .withInput(new LongWritable(1), new Text("2,GE"))
                .withInput(new LongWritable(1), new Text("3,GB"))
                .withInput(new LongWritable(1), new Text("1,LT"))

                .withOutput(new Text("1"), new Text("country:LT"))
                .withOutput(new Text("2"), new Text("country:GE"))
                .withOutput(new Text("3"), new Text("country:GB"))
                .withOutput(new Text("1"), new Text("country:LT"))

                //assert
                .runTest();
    }

    @Test
    public void testUserClickReducer() throws IOException {
        //setup
        var reducer = new UserClickReducer();
        reduceJoinDriver.setReducer(reducer);
        var c = new Configuration();
        c.set("countryRegex", "LT");
        reduceJoinDriver.setConfiguration(c);

        var values = new ArrayList<Text>();
        values.add(new Text("country:LT"));
        values.add(new Text("click:2020.01.01,1,/item/delete"));

        reduceJoinDriver.withInput(new Text("1"), values);

        values = new ArrayList<>();
        values.add(new Text("country:GB"));
        values.add(new Text("click:2020.01.01,2,/items/search"));

        reduceJoinDriver.withInput(new Text("2"), values)
                .withOutput(new Text("2020.01.01"), new Text("1 /item/delete"))

                //assert
                .runTest();
    }

    @Test
    public void testSumReducer() throws IOException {
        //setup
        var reducer = new SumReducer();
        reduceDriver.setReducer(reducer);

        var values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));

        reduceDriver.withInput(new Text("2020.01.01"), values)
                .withOutput(new Text("2020.01.01"), new IntWritable(2))

                //assert
                .runTest();
    }

    @Test
    public void testClickCount() throws IOException {
        //setup
        var mapper = new ClickMapper();
        var reducer = new SumReducer();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);

        mapReduceDriver.withInput(new LongWritable(1), new Text("2020.01.01"))
                .withInput(new LongWritable(1), new Text("2020.01.01"))
                .withInput(new LongWritable(1), new Text("2020.01.02"))

                .withOutput(new Text("2020.01.01"), new IntWritable(2))
                .withOutput(new Text("2020.01.02"), new IntWritable(1))

                //assert
                .runTest();
    }

    @Test
    public void testUserClickFilter() throws IOException {
        //setup
        var userMapper = new UserMapper();
        var userClickMapper = new UserClickMapper();
        var reducer = new UserClickReducer();
        mapReduceJoinDriver.addMapper(userMapper);
        mapReduceJoinDriver.addMapper(userClickMapper);
        mapReduceJoinDriver.setReducer(reducer);
        var c = new Configuration();
        c.set("countryRegex", "LT");
        mapReduceJoinDriver.setConfiguration(c);

        mapReduceJoinDriver.withInput(userMapper, new LongWritable(1), new Text("1,LT"))
                .withInput(userMapper, new LongWritable(1), new Text("2,GE"))
                .withInput(userMapper, new LongWritable(1), new Text("3,GB"));

        mapReduceJoinDriver.withInput(userClickMapper, new LongWritable(1), new Text("2020.01.01,1,/profile/view"))
                .withInput(userClickMapper, new LongWritable(1), new Text("2020.01.01,2,/items/search"))
                .withInput(userClickMapper, new LongWritable(1), new Text("2020.01.02,3,/item/add"))
                .withInput(userClickMapper, new LongWritable(1), new Text("2020.01.01,1,/item/delete"))

                .withOutput(new Text("2020.01.01"), new Text("1 /profile/view"))
                .withOutput(new Text("2020.01.01"), new Text("1 /item/delete"))

                //assert
                .runTest();
    }
}
