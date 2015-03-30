package com.baina.game.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by jjhu on 2015/3/19.
 */
public class TestImsi {
    public static class TestImsiMapper extends Mapper<Object, Text, Text, IntWritable>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String []items = value.toString().split("\\|");
            if (items.length != 27 && items.length != 28)       //日志有点问题，前期日志长度为28
                return;

            try {
                Long.parseLong(items[4]);
                if (CommonService.isTrueImsi(items[10])){
                    String MNC = items[10].substring(3, 5);
                    context.write(new Text(MNC), new IntWritable(1));
                }
                return;
            }catch (Exception e){
                e.printStackTrace();
            }
            if (CommonService.isTrueImsi(items[9])){
                String MNC = items[9].substring(3, 5);
                context.write(new Text(MNC), new IntWritable(1));
            }
        }
    }

    public static class TestImsiReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values){
                sum += i.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "TestImsi");
        job.setJarByClass(TestImsi.class);
        job.setMapperClass(TestImsiMapper.class);
        job.setCombinerClass(TestImsiReducer.class);
        job.setReducerClass(TestImsiReducer.class);

        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path out = new Path("/tmp/AllImsiMNC");
        FileInputFormat.addInputPath(job, new Path("/data/rawlog/card/pay_for_order/2015-02-*"));
        FileInputFormat.addInputPath(job, new Path("/data/rawlog/card/pay_for_order/2015-03-*"));
        FileOutputFormat.setOutputPath(job, out);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out)){
            fs.delete(out);
        }

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
