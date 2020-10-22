package com.tongyongtao.BigData.MapReduce.FileInputformatCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: tongyongtao
 * Date: 2020-10-08
 * Time: 20:29
 * 对FileInputformatCase中TestInputformatCase的简单实现(默认)
 * 案例统计字母出现的个数
 */
public class MR_WordCount1 {
    static Configuration con;
    static IntWritable wordtimes = new IntWritable(1);
    static Text text = new Text();

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"C:\\Users\\hp\\IdeaProjects\\GitHub_Maven\\src\\main\\resources\\a.txt", "C:\\MapReduce3"};
        con = new Configuration();
        Job job = Job.getInstance(con, "");
        job.setMapperClass(Map_MR_WordCount1.class);
        job.setReducerClass(Reduce_MR_WordCount1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(1);
        System.out.println(job.waitForCompletion(true) ? "正确" : "错误");

    }

    public static class Map_MR_WordCount1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            for (String word : value.toString().split("\\s+")) {
                text.set(word);
                context.write(text, wordtimes);
            }
        }
    }

    public static class Reduce_MR_WordCount1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int num = 0;
            for (IntWritable value : values) {
                num++;
            }
            context.write(key, new IntWritable(num));
        }
    }

}
