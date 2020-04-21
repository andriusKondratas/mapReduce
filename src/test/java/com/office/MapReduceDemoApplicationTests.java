package com.office;


import com.office.mapper.ClickMapper;
import com.office.mapper.UserClickFilterMapper;
import com.office.reducer.SumReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;


class MapReduceDemoApplicationTests {

    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    MapDriver<Object, Text, NullWritable, Text> mapFilterDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
    Configuration conf;
    Mapper.Context context;

    @BeforeEach
    public void setUp() {
        mapDriver = new MapDriver<>();
        mapFilterDriver = new MapDriver<>();
        reduceDriver = new ReduceDriver<>();
        mapReduceDriver = new MapReduceDriver<>();
        conf = mock(Configuration.class);
        context = mock(Mapper.Context.class);
        when(context.getConfiguration()).thenReturn(conf);
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
    public void testUserClickFilterMapper() throws IOException {
        //setup
        UserClickFilterMapper mapper = new UserClickFilterMapper();
        conf.set("countryRegex", "1");
        when(conf.get(anyString())).thenReturn("1");
        mapper.setup(context);
        mapFilterDriver.setMapper(mapper);

        mapFilterDriver.withInput(new LongWritable(1), new Text("2020.01.01,1,/profile/view"));
        mapFilterDriver.withInput(new LongWritable(1), new Text("2020.01.01,2,/items/search"));
        mapFilterDriver.withInput(new LongWritable(1), new Text("2020.01.02,3,/item/add"));
        mapFilterDriver.withInput(new LongWritable(1), new Text("2020.01.01,1,/item/delete"));

        mapFilterDriver.withOutput(NullWritable.get(), new Text("2020.01.01,1,/profile/view"));
        mapFilterDriver.withOutput(NullWritable.get(), new Text("2020.01.01,1,/item/delete"));

        //assert
        mapFilterDriver.runTest();
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
