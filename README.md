### Prerequisites:
* Maven installed & configured, e.g: `sudo apt install maven`
* Any Open / Oracle / .. JDK installed, e.g : `sudo apt install default-jdk`
* Repository cloned `git clone https://github.com/officeI/mapReduce.git ${repository location}`
* Build .jar file
```
   cd '${repository location}'
   mvn install
   mvn clean package
```
* Hadoop installed & configured, e.g : [Digitalocean](https://www.digitalocean.com/community/tutorials/how-to-install-hadoop-in-stand-alone-mode-on-ubuntu-18-04)

### Task #1: use implemented map-reduce framework for aggregation
Having in mind `data/clicks` dataset with "date" column, count how many clicks there were for each date and write the results to `data/total_clicks` dataset with "date" and "count" columns.
* Prepare data
```
   mkdir ~/data
   mkdir ~/data/clicks
   cp -v ${repository location}/mapReduce/src/test/resources/clicks/* ~/data/clicks/
```
* Run Hadoop job
```
   /usr/local/hadoop/bin/hadoop jar ${repository location}/mapReduce/target/map-reduce-demo-0.0.1-SNAPSHOT-jar-with-dependencies.jar ClickCount ~/data/clicks/ ~/data/total_clicks &> ~/log.txt
   less ~/log.txt
    Homework is fun game !
    Will execute ClickCount case
    2020-04-20 21:01:23,079 INFO impl.MetricsConfig: loaded properties from hadoop-metrics2.properties
    ...
    2020-04-20 21:01:24,893 INFO mapreduce.Job: Job job_local1217207120_0001 running in uber mode : false
    2020-04-20 21:01:24,895 INFO mapreduce.Job:  map 100% reduce 100%
    2020-04-20 21:01:24,897 INFO mapreduce.Job: Job job_local1217207120_0001 completed successfully
    2020-04-20 21:01:24,910 INFO mapreduce.Job: Counters: 30
    ...
   cat ~/data/total_clicks/*
    2020.01.01	5
    2020.01.02	12
    2020.01.03	6
    2020.01.04	6
    2020.01.05	1
    2020.01.06	3
    2020.01.07	2
    2020.01.08	1
``` 