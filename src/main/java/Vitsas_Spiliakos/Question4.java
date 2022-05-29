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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Random;
import java.util.concurrent.TimeUnit;



public class Question4 {


    public static class MoneyPerPurchaseMapper extends Mapper<LongWritable, Text, IntWritable, Q4WritableComparable> {

        public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {
            String[] lineContent = record.toString().split(",");
            Q4WritableComparable data = new Q4WritableComparable();
            data.setID(new IntWritable(Integer.parseInt(lineContent[0])));
            data.setMntWines(new FloatWritable(Float.parseFloat(lineContent[9])));
            data.setMntFruits(new FloatWritable(Float.parseFloat(lineContent[10])));
            data.setMntMeatProducts(new FloatWritable(Float.parseFloat(lineContent[11])));
            data.setMntFishProducts(new FloatWritable(Float.parseFloat(lineContent[12])));
            data.setMntSweetProducts(new FloatWritable(Float.parseFloat(lineContent[13])));
            data.setMntGoldProds(new FloatWritable(Float.parseFloat(lineContent[14])));
            data.setNumWebPurchases(new IntWritable(Integer.parseInt(lineContent[16])));
            data.setNumCatalogPurchases(new IntWritable(Integer.parseInt(lineContent[17])));
            data.setNumStorePurchases(new IntWritable(Integer.parseInt(lineContent[18])));


            //create a random key number between 1-4
            int max = 4;
            int min = 1;
            Random r = new Random();
            int bucket = r.nextInt(max) + min;

            context.write(new IntWritable(bucket), data);

        }
    }

    public static class MoneyPerPurchaseReducer extends Reducer<IntWritable, Q4WritableComparable, IntWritable, FloatWritable> {

        public void reduce(IntWritable key, Iterable<Q4WritableComparable> values, Context context) throws IOException, InterruptedException {
            float sum = 0.0f;
            int size = 0;

            for (Q4WritableComparable val : values) {
                sum += val.getMoneyPerPurchase().get();
                size++;
            }
            context.write(new IntWritable(size), new FloatWritable(sum));
        }
    }


    public static class FindMeanMoneySpentMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {
            context.write(one, record);
        }

    }

    public static class FindMeanMoneySpentReducer extends Reducer<IntWritable, Text, Text, FloatWritable> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float sum = 0.0f;
            int count = 0;
            for (Text val : values) {
                String value = val.toString();
                String[] tmp = value.split("\\s+");
                count += Integer.parseInt(tmp[0]);
                sum += Float.parseFloat(tmp[1]);
            }
            context.write(new Text("Mean"), new FloatWritable(Math.round(sum/count)));
        }

    }


    public static class FindResultMapper extends Mapper<LongWritable, Text, Q4WritableComparable, IntWritable> {
        public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {
            String[] lineContent = record.toString().split(",");
            Q4WritableComparable data = new Q4WritableComparable();
            data.setID(new IntWritable(Integer.parseInt(lineContent[0])));
            data.setIncome(new FloatWritable(Float.parseFloat(lineContent[4])));
            data.setDt_Customer(new Text(lineContent[7]));
            data.setMntWines(new FloatWritable(Float.parseFloat(lineContent[9])));
            data.setMntFruits(new FloatWritable(Float.parseFloat(lineContent[10])));
            data.setMntMeatProducts(new FloatWritable(Float.parseFloat(lineContent[11])));
            data.setMntFishProducts(new FloatWritable(Float.parseFloat(lineContent[12])));
            data.setMntSweetProducts(new FloatWritable(Float.parseFloat(lineContent[13])));
            data.setMntGoldProds(new FloatWritable(Float.parseFloat(lineContent[14])));
            data.setNumWebPurchases(new IntWritable(Integer.parseInt(lineContent[16])));
            data.setNumCatalogPurchases(new IntWritable(Integer.parseInt(lineContent[17])));
            data.setNumStorePurchases(new IntWritable(Integer.parseInt(lineContent[18])));


            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            String currentDayString = sdf.format(Calendar.getInstance().getTime());

            String[] registration = data.getDt_Customer().toString().split("-");
            Calendar cal = new GregorianCalendar(Integer.parseInt(registration[0]),Integer.parseInt(registration[1])-1 , Integer.parseInt(registration[2]));
            String regDay = sdf.format(cal.getTime());


            try {
                Date date1 = sdf.parse(regDay);
                Date date2 = sdf.parse(currentDayString);

                long diff = date2.getTime() - date1.getTime();
                float convertedDiff = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);

                if ((data.getIncome().get() > 69500) && (data.getMoneyPerPurchase().get() > (1.5 * Float.parseFloat(context.getConfiguration().get("mean"))))){
                    if (convertedDiff < 365){
                        data.setCategory(new Text("Gold"));
                        context.write(data, data.getID());
                    }else {
                        data.setCategory(new Text("Silver"));
                        context.write(data, data.getID());
                    }
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }
    }

    public static class FindResultReducer extends Reducer<Q4WritableComparable, IntWritable, Text, Text> {
        public void reduce(Q4WritableComparable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            StringBuilder concated = new StringBuilder();
            for (IntWritable val: values){

                concated.append(val).append(", ");
            }
            context.write(key.getCategory(), new Text(concated.toString()));
        }
    }



    public static void main(String[] args) throws Exception {

        if (args.length != 4 ){
            System.err.println ("Usage :<inputlocation1> <outputlocation1> <outputlocation2> <outputlocation3>");
            System.exit(0);
        }

        //Job1 - FindSumCount
        Configuration confMoneyPerPurchase = new Configuration();
        //Use first configuration to delete folders in hdfs if they exist.
        String[] files=new GenericOptionsParser(confMoneyPerPurchase,args).getRemainingArgs();
        Path input=new Path(files[0]);
        Path output1=new Path(files[1]);
        Path output2=new Path(files[2]);
        Path output3=new Path(files[3]);
        FileSystem fs = FileSystem.get(confMoneyPerPurchase);
        if(fs.exists(output1)){
            fs.delete(output1, true);
        }
        if(fs.exists(output2)){
            fs.delete(output2, true);
        }
        if(fs.exists(output3)){
            fs.delete(output3, true);
        }

        Job jobMoneyPerPurchase = Job.getInstance(confMoneyPerPurchase, "Question4 MoneyPerPurchase Job");
        jobMoneyPerPurchase.setJarByClass(Question4.class);
        jobMoneyPerPurchase.setMapperClass(MoneyPerPurchaseMapper.class);
        jobMoneyPerPurchase.setMapOutputKeyClass(IntWritable.class);
        jobMoneyPerPurchase.setMapOutputValueClass(Q4WritableComparable.class);

        jobMoneyPerPurchase.setReducerClass(MoneyPerPurchaseReducer.class);
        jobMoneyPerPurchase.setOutputKeyClass(IntWritable.class);
        jobMoneyPerPurchase.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(jobMoneyPerPurchase, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobMoneyPerPurchase, new Path(args[1]));
        jobMoneyPerPurchase.waitForCompletion(true);


        //Job2 - FindMeanMoneySpent
        Configuration confFindMeanMoneySpent = new Configuration();
        Job jobFindMeanMoneySpent = Job.getInstance(confFindMeanMoneySpent, "Question4 FindMeanMoneySpent Job");
        jobFindMeanMoneySpent.setJarByClass(Question4.class);
        jobFindMeanMoneySpent.setMapperClass(Question4.FindMeanMoneySpentMapper.class);
        jobFindMeanMoneySpent.setMapOutputKeyClass(IntWritable.class);
        jobFindMeanMoneySpent.setMapOutputValueClass(Text.class);

        jobFindMeanMoneySpent.setReducerClass(Question4.FindMeanMoneySpentReducer.class);
        jobFindMeanMoneySpent.setOutputKeyClass(Text.class);
        jobFindMeanMoneySpent.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(jobFindMeanMoneySpent, new Path(args[1]));
        FileOutputFormat.setOutputPath(jobFindMeanMoneySpent, new Path(args[2]));
        jobFindMeanMoneySpent.waitForCompletion(true);



        //Job3 FindResult
        Path job2result = new Path("output2/part-r-00000");
        FSDataInputStream inputStream = fs.open(job2result);

        String output2File = org.apache.commons.io.IOUtils.toString(inputStream,"UTF-8");
        String[] mean = output2File.split("\\s+");

        Configuration confFindResult = new Configuration();
        confFindResult.set("mean",mean[1]);

        Job jobFindResult = Job.getInstance(confFindResult, "Question4 FindResult Job");
        jobFindResult.setJarByClass(Question4.class);
        jobFindResult.setMapperClass(Question4.FindResultMapper.class);
        jobFindResult.setMapOutputKeyClass(Q4WritableComparable.class);
        jobFindResult.setMapOutputValueClass(IntWritable.class);

        jobFindResult.setReducerClass(Question4.FindResultReducer.class);
        jobFindResult.setOutputKeyClass(Text.class);
        jobFindResult.setOutputValueClass(Text.class);

        jobFindResult.setPartitionerClass(Q4Partioner.class);
        jobFindResult.setGroupingComparatorClass(Q4GroupingComparator.class);

        FileInputFormat.addInputPath(jobFindResult, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobFindResult, new Path(args[3]));
        jobFindResult.waitForCompletion(true);

    }


}
