package Vitsas_Spiliakos;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import java.io.IOException;
import java.util.Random;



public class Question3 {

    //Input key type, input value type,output key type, output value type
    public static class FindSumCountMapper extends Mapper<LongWritable, Text, IntWritable, Q3WritableComparable> {

        public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {
            String[] lineContent = record.toString().split(",");
            Q3WritableComparable data = new Q3WritableComparable();

            //data.setID(new IntWritable(Integer.parseInt(lineContent[0])));
            //data.setYearBirth(new IntWritable(Integer.parseInt(lineContent[1])));
            //data.setEducation(new Text(lineContent[2]));
            //data.setMaritalStatus(new Text(lineContent[3]));
            //data.setIncome(new FloatWritable(Float.parseFloat(lineContent[4])));
            data.setMntWines(new FloatWritable(Float.parseFloat(lineContent[9])));

            //create a random key number between 1-4
            int max = 4;
            int min = 1;
            Random r = new Random();
            int bucket = r.nextInt(max) + min;

            context.write(new IntWritable(bucket), data);

        }
    }

    public static class FindSumCountReducer extends Reducer<IntWritable, Q3WritableComparable, IntWritable, FloatWritable> {

        public void reduce(IntWritable key, Iterable<Q3WritableComparable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int size = 0;

            for (Q3WritableComparable val : values) {
                sum += val.getMntWines().get();
                size++;
            }
            context.write(new IntWritable(size), new FloatWritable(sum));
        }
    }


    public static class FindMeanMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {
            context.write(one, record);
        }

    }

    public static class FindMeanReducer extends Reducer<IntWritable, Text, Text, FloatWritable> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for (Text val : values) {
                String value = val.toString();
                String[] tmp = value.split("\\s+");
                count += Integer.parseInt(tmp[0]);
                sum += Float.parseFloat(tmp[1]);
            }
            context.write(new Text("Mean"), new FloatWritable(sum/count));
        }

    }


    public static class FindResultMapper extends Mapper<LongWritable, Text, Q3WritableComparable, NullWritable> {
        public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {
            String[] lineContent = record.toString().split(",");

            Q3WritableComparable data = new Q3WritableComparable();

            data.setID(new IntWritable(Integer.parseInt(lineContent[0])));
            data.setYearBirth(new IntWritable(Integer.parseInt(lineContent[1])));
            data.setEducation(new Text(lineContent[2]));
            data.setMaritalStatus(new Text(lineContent[3]));
            data.setIncome(new FloatWritable(Float.parseFloat(lineContent[4])));
            data.setMntWines(new FloatWritable(Float.parseFloat(lineContent[9])));

            if( data.getMntWines().get() > (1.5 * Float.parseFloat(context.getConfiguration().get("mean")))){
                context.write(data, NullWritable.get());
            }

        }
    }

    public static class FindResultReducer extends Reducer<Q3WritableComparable, NullWritable, IntWritable, Q3WritableComparable> {
        private final static IntWritable one = new IntWritable(1);
        public void reduce(Q3WritableComparable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            context.write(one, key);
        }
    }


    public static class AddRankMapper extends Mapper<LongWritable, Text, IntWritable, Q3WritableComparable> {

        public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {
            IntWritable one = new IntWritable(1);
            String[] tmp = record.toString().split("\\t+");
            Q3WritableComparable data = new Q3WritableComparable();

            data.setRank(new IntWritable(Integer.parseInt(tmp[1])));
            data.setID(new IntWritable(Integer.parseInt(tmp[2])));
            data.setAge(new IntWritable(Integer.parseInt(tmp[3])));
            data.setEducation(new Text(tmp[4]));
            data.setMaritalStatus(new Text(tmp[5]));
            data.setIncome(new FloatWritable(Float.parseFloat(tmp[6])));
            data.setMntWines(new FloatWritable(Float.parseFloat(tmp[7])));

            context.write(one, data);

        }
    }

    public static class AddRankReducer extends Reducer<IntWritable, Q3WritableComparable, Q3WritableComparable , NullWritable> {
        public void reduce(IntWritable key, Iterable<Q3WritableComparable> values, Context context) throws IOException, InterruptedException {
            int counter = 1;
            for (Q3WritableComparable val : values) {
                val.setRank(new IntWritable(counter));
                counter ++;
                context.write(val,  NullWritable.get());
            }

        }
    }





    public static void main(String[] args) throws Exception {

        if (args.length != 5 ){
            System.err.println ("Usage :<inputlocation1> <outputlocation1> <outputlocation2> <outputlocation3> <outputlocation4>");
            System.exit(0);
        }

        //Job1 - FindSumCount
        Configuration confSumCount = new Configuration();
        //Use first configuration to delete folders in hdfs if they exist.
        String[] files=new GenericOptionsParser(confSumCount,args).getRemainingArgs();
        Path input=new Path(files[0]);
        Path output1=new Path(files[1]);
        Path output2=new Path(files[2]);
        Path output3=new Path(files[3]);
        Path output4=new Path(files[4]);
        FileSystem fs = FileSystem.get(confSumCount);
        if(fs.exists(output1)){
            fs.delete(output1, true);
        }
        if(fs.exists(output2)){
            fs.delete(output2, true);
        }
        if(fs.exists(output3)){
            fs.delete(output3, true);
        }
        if(fs.exists(output4)){
            fs.delete(output4, true);
        }
        Job jobSumCount = Job.getInstance(confSumCount, "Question3 FindSumCount Job");
        jobSumCount.setJarByClass(Question3.class);
        jobSumCount.setMapperClass(FindSumCountMapper.class);
        jobSumCount.setMapOutputKeyClass(IntWritable.class);
        jobSumCount.setMapOutputValueClass(Q3WritableComparable.class);

        jobSumCount.setReducerClass(FindSumCountReducer.class);
        jobSumCount.setOutputKeyClass(IntWritable.class);
        jobSumCount.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(jobSumCount, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobSumCount, new Path(args[1]));
        jobSumCount.waitForCompletion(true);


        //Job2 - FindMean
        Configuration confFindMean = new Configuration();
        Job jobFindMean = Job.getInstance(confFindMean, "Question3 FindMean Job");
        jobFindMean.setJarByClass(Question3.class);
        jobFindMean.setMapperClass(FindMeanMapper.class);
        jobFindMean.setMapOutputKeyClass(IntWritable.class);
        jobFindMean.setMapOutputValueClass(Text.class);

        jobFindMean.setReducerClass(FindMeanReducer.class);
        jobFindMean.setOutputKeyClass(Text.class);
        jobFindMean.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(jobFindMean, new Path(args[1]));
        FileOutputFormat.setOutputPath(jobFindMean, new Path(args[2]));
        jobFindMean.waitForCompletion(true);



        //Job3 - FindResult
        Path job2result = new Path("output2/part-r-00000");
        FSDataInputStream inputStream = fs.open(job2result);

        String output2File = org.apache.commons.io.IOUtils.toString(inputStream,"UTF-8");
        String[] mean = output2File.split("\\s+");

        Configuration confFindResult = new Configuration();
        confFindResult.set("mean",mean[1]);

        Job jobFindResult = Job.getInstance(confFindResult, "Question3 FindResult Job");
        jobFindResult.setJarByClass(Question3.class);
        jobFindResult.setMapperClass(FindResultMapper.class);
        jobFindResult.setMapOutputKeyClass(Q3WritableComparable.class);
        jobFindResult.setMapOutputValueClass(NullWritable.class);

        jobFindResult.setReducerClass(FindResultReducer.class);
        jobFindResult.setOutputKeyClass(IntWritable.class);
        jobFindResult.setOutputValueClass(Q3WritableComparable.class);
        FileInputFormat.addInputPath(jobFindResult, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobFindResult, new Path(args[3]));
        jobFindResult.waitForCompletion(true);

        //Job4 AddRank
        Configuration confAddRank = new Configuration();
        Job jobAddRank = Job.getInstance(confAddRank, "Question3 AddRank Job");
        jobAddRank.setJarByClass(Question3.class);
        jobAddRank.setMapperClass(AddRankMapper.class);
        jobAddRank.setMapOutputKeyClass(IntWritable.class);
        jobAddRank.setMapOutputValueClass(Q3WritableComparable.class);

        jobAddRank.setReducerClass(AddRankReducer.class);
        jobAddRank.setOutputKeyClass(Q3WritableComparable.class);
        jobAddRank.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(jobAddRank, new Path(args[3]));
        FileOutputFormat.setOutputPath(jobAddRank, new Path(args[4]));
        System.exit(jobAddRank.waitForCompletion(true) ? 0 : 1);

    }


}
