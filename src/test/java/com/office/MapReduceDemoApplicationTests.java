package com.office;


import com.office.mapper.ClickMapper;
import com.office.reducer.SumReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class MapReduceDemoApplicationTests {
    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @BeforeEach
    public void setUp() {
        mapDriver = new MapDriver<>();
        reduceDriver = new ReduceDriver<>();
        mapReduceDriver = new MapReduceDriver<>();
    }

    @Test
    public void testClickMapper() throws IOException {
        //setup
        ClickMapper mapper = new ClickMapper();
        mapDriver.setMapper(mapper);

        mapDriver.withInput(new LongWritable(1), new Text("2020.01.01"));
        mapDriver.withInput(new LongWritable(1), new Text("2020.01.01"));
        mapDriver.withInput(new LongWritable(1), new Text("2020.01.02"));

        mapDriver.withOutput(new Text("2020.01.01"), new IntWritable(1));
        mapDriver.withOutput(new Text("2020.01.01"), new IntWritable(1));
        mapDriver.withOutput(new Text("2020.01.02"), new IntWritable(1));

        //assert
        mapDriver.runTest();
    }

    @Test
    public void testSumReducer() throws IOException {
        //setup
        SumReducer reducer = new SumReducer();
        reduceDriver.setReducer(reducer);

        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));

        reduceDriver.withInput(new Text("2020.01.01"), values);
        reduceDriver.withOutput(new Text("2020.01.01"), new IntWritable(2));

        //assert
        reduceDriver.runTest();
    }

    @Test
    public void testClickCount() throws IOException {
        //setup
        ClickMapper mapper = new ClickMapper();
        SumReducer reducer = new SumReducer();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);

        mapReduceDriver.withInput(new LongWritable(1), new Text("2020.01.01"));
        mapReduceDriver.withInput(new LongWritable(1), new Text("2020.01.01"));
        mapReduceDriver.withInput(new LongWritable(1), new Text("2020.01.02"));

        mapReduceDriver.addOutput(new Text("2020.01.01"), new IntWritable(2));
        mapReduceDriver.addOutput(new Text("2020.01.02"), new IntWritable(1));

        //assert
        mapReduceDriver.runTest();
    }
}
