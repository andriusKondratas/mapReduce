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
### Solution #1:
* Prepare data
```
   mkdir ~/dataS1
   mkdir ~/dataS1/clicks
   cp -v ${repository location}/mapReduce/src/test/resources/clicks/* ~/dataS1/clicks/
```
* Run Hadoop job
```
   /usr/local/hadoop/bin/hadoop jar ${repository location}/mapReduce/target/map-reduce-demo-0.0.1-SNAPSHOT-jar-with-dependencies.jar ClickCount ~/dataS1/clicks/ ~/dataS1/total_clicks &> ~/log.txt
   less ~/log.txt
    Will execute ClickCount case
    2020-04-20 21:01:23,079 INFO impl.MetricsConfig: loaded properties from hadoop-metrics2.properties
    ...
    2020-04-20 21:01:24,893 INFO mapreduce.Job: Job job_local1217207120_0001 running in uber mode : false
    2020-04-20 21:01:24,895 INFO mapreduce.Job:  map 100% reduce 100%
    2020-04-20 21:01:24,897 INFO mapreduce.Job: Job job_local1217207120_0001 completed successfully
    2020-04-20 21:01:24,910 INFO mapreduce.Job: Counters: 30
    ...
   cat ~/dataS1/total_clicks/*
    2020.01.01	5
    2020.01.02	12
    2020.01.03	6
    2020.01.04	6
    2020.01.05	1
    2020.01.06	3
    2020.01.07	2
    2020.01.08	1
```
### Task #2: join two datasets using implemented map-reduce framework
There are two datasets:

- `data/users` dataset with columns "id" and "country"
- `data/clicks` dataset with columns "date", "user_id" and "click_target"

We'd like to produce a new dataset called `data/filtered_clicks` that includes only those clicks that belong to users from Lithuania (`country=LT`).
### Solution #2:
* Prepare data
```
   mkdir ~/dataS2
   mkdir ~/dataS2/clicks
   mkdir ~/dataS2/users
   cp -v ${repository location}/mapReduce/src/test/resources/filterUsers/clicks/* ~/dataS2/clicks/
   cp -v ${repository location}/mapReduce/src/test/resources/filterUsers/users/* ~/dataS2/users/
```
* Run Hadoop job
```
   /usr/local/hadoop/bin/hadoop jar ${repository location}/mapReduce/target/map-reduce-demo-0.0.1-SNAPSHOT-jar-with-dependencies.jar ClickFilter LT ~/dataS2/users/ ~/dataS2/clicks/ ~/dataS2/filtered_clicks &> ~/log.txt
   less ~/log.txt
    Will execute ClickFilter case
    2020-04-21 17:56:08,573 INFO impl.MetricsConfig: loaded properties from hadoop-metrics2.properties
    2020-04-21 17:56:08,783 INFO impl.MetricsSystemImpl: Scheduled Metric snapshot period at 10 second(s).
    ...
    22020-04-21 17:56:10,421 INFO mapred.LocalJobRunner: Finishing task: attempt_local1690557734_0001_r_000000_0
    2020-04-21 17:56:10,422 INFO mapred.LocalJobRunner: reduce task executor complete.
    2020-04-21 17:56:10,783 INFO mapreduce.Job: Job job_local1690557734_0001 running in uber mode : false
    2020-04-21 17:56:10,785 INFO mapreduce.Job:  map 100% reduce 100%
    2020-04-21 17:56:10,787 INFO mapreduce.Job: Job job_local1690557734_0001 completed successfully
    2020-04-21 17:56:10,800 INFO mapreduce.Job: Counters: 30
    ...
   cat ~/dataS2/filtered_clicks*
    2020.01.07	1 /items/search
    2020.01.07	1 /items/search
    2020.01.04	1 /profile/view
    2020.01.03	1 /item/forget
    2020.01.01	1 /profile/view
    2020.01.07	11 /items/search
    2020.01.07	11 /items/search
    2020.01.04	11 /profile/view
    2020.01.03	11 /item/forget
    2020.01.01	11 /profile/view
``` 