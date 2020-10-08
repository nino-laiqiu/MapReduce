package com.tongyongtao.BigData.MapReduce.PartitionAndGrouping;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User:tongyongtao
 * Date: 2020-10-08
 * Time: 10:52
 * 重写分区分组方法
 * 案例:从众多的电影评论中获取每个用户评分排名全三的电影,按电影名排序,电影名相同采用评分排序
 */
public class MR_PartitionAndGroup {
    static Configuration con;
    static Gson gson = new Gson();

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"C:\\Users\\hp\\IdeaProjects\\GitHub_Maven\\src\\main\\resources\\test.json", "C:\\MapReduce"};
        con = new Configuration();
        Job job = Job.getInstance(con, "MR_PartitionAndGroup");
        //设置来源
        job.setMapperClass(Map_PartitionAndGroup.class);
        job.setReducerClass(Reduce_PartitionAndGroup.class);
        //设置输入 输出类型
        job.setMapOutputKeyClass(Movie.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Movie.class);
        job.setOutputValueClass(NullWritable.class);
        //设置分区标准
        job.setPartitionerClass(MyPartion.class);
        //设置分组标准
        job.setGroupingComparatorClass(MyComparator.class);
        //设置数据 输入 输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //设置reduce的个数
        job.setNumReduceTasks(3);
        //关闭
        System.out.println(job.waitForCompletion(true) ? "true" : "false");
    }

    public static class Map_PartitionAndGroup extends Mapper<LongWritable, Text, Movie, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                Movie movie = gson.fromJson(value.toString(), Movie.class);
                context.write(movie, NullWritable.get());
            } catch (Exception e) {
                //捕捉异常,不处理异常
            }
        }
    }

    public static class Reduce_PartitionAndGroup extends Reducer<Movie, NullWritable, Movie, NullWritable> {
        @Override
        protected void reduce(Movie key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int num = 0;
            for (NullWritable value : values) {
                context.write(key, NullWritable.get());
                num++;
                if (num == 3) {
                    return;
                }
            }
        }
    }

    //重写分区方法,按uid分区
    public static class MyPartion extends Partitioner<Movie, NullWritable> {
        @Override
        public int getPartition(Movie movie, NullWritable nullWritable, int i) {
            //按原码来,其实可自定义
            return (movie.getUid().hashCode() & Integer.MAX_VALUE) % i;
        }
    }

    //重写分组方法 按uid来排序
    public static class MyComparator extends WritableComparator {
        public MyComparator() {
            super(Movie.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Movie A = (Movie) a;
            Movie B = (Movie) b;
            return A.getUid().compareTo(B.getUid());
        }
    }
}
