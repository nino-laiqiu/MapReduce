package com.tongyongtao.BigData.MapReduce.PartitionAndGrouping;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: tongyongtao
 * Date: 2020-10-08
 * Time: 16:30
 * 使用set方法在处理reduce之后的工作
 * 案例:获取电影评论数排名前10的电影
 */
public class MR_PartitionAndGroup1 {
    static Configuration con;
    static Gson gson = new Gson();


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"C:\\Users\\hp\\IdeaProjects\\GitHub_Maven\\src\\main\\resources\\test.json", "C:\\MapReduce2"};
        con = new Configuration();
        Job job = Job.getInstance(con, "MR_PartitionAndGroup1");
        job.setMapperClass(Map_PartitionAndGroup1.class);
        job.setReducerClass(Reduce_PartitionAndGroup1.class);
        job.setMapOutputKeyClass(Movie1.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Movie1.class);
        job.setOutputValueClass(IntWritable.class);
        job.setPartitionerClass(MyPartion.class);
        job.setGroupingComparatorClass(MyComparator.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //设置reduce数量最好为1
        job.setNumReduceTasks(2);
        System.out.println(job.waitForCompletion(true) ? "true" : "false");

    }

    public static class Map_PartitionAndGroup1 extends Mapper<LongWritable, Text, Movie1, IntWritable> {
        //  IntWritable intWritable = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                Movie1 movie = gson.fromJson(value.toString(), Movie1.class);
                context.write(movie, new IntWritable(1));
            } catch (Exception e) {

            }
        }
    }

    static Map<Movie1, Integer> movie_topn = new HashMap<>();

    public static class Reduce_PartitionAndGroup1 extends Reducer<Movie1, IntWritable, Movie1, IntWritable> {


        @Override
        protected void reduce(Movie1 key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //reducetask拉取maptask端 聚合排序每一组相同的key在一个迭代器中
            //这里注意迭代器只有一个对象,采用赋值或者克隆的方法
            // BeanUtils.copyProperties();同样也可
            Movie1 movie = null;
            try {
                movie = (Movie1) key.clone();
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
            int sum = 0;
            for (IntWritable value : values) {


                sum++;
            }
            // context.write(key,new IntWritable(sum));
            movie_topn.put(movie, sum);

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //set方法最终执行 有且执行一次,采取构建一个集合存储
            //hashmap集合中存储 key是movie 值是数量  进行排序取值
            List<Map.Entry<Movie1, Integer>> TopNmovies = new ArrayList<>(movie_topn.entrySet());
            TopNmovies.sort((t1, t2) -> Integer.compare(t2.getValue(), t1.getValue()));
            for (int i = 0; i < Integer.min(3, TopNmovies.size()); i++) {
                context.write(TopNmovies.get(i).getKey(), new IntWritable(TopNmovies.get(i).getValue()));
            }
        }
    }

    //按movie来分区
    public static class MyPartion extends Partitioner<Movie1, IntWritable> {
        @Override
        public int getPartition(Movie1 movie_topn, IntWritable intWritable, int i) {
            return (movie_topn.getMovie().hashCode() & Integer.MAX_VALUE) % i;
        }
    }

    //按movie来排序
    public static class MyComparator extends WritableComparator {
        public MyComparator() {
            super(Movie1.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Movie1 A = (Movie1) a;
            Movie1 B = (Movie1) b;
            return A.getMovie().compareTo(B.getMovie());
        }
    }
}
