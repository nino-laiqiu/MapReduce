package com.tongyongtao.BigData.MapReduce.JoinCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: tongyongtao
 * Date: 2020-10-08
 * Time: 22:32
 * 简单join的实现 两张表  实现在reduce端的join
 * 首先在setup方法中获取文件的名字 比如 order.txt 和 pd.txt
 * key 和 value 的设置,两张表有重合部分作为key 这样排序后在同一个迭代器中
 * 集合遍历取代原来的
 * order.txt  id pid amount
 * pd.txt   pid pname
 * ...这个案例检查了好久 出现如下bug :dataInput和 dataOutput写入读取的顺序不一致,spilt[]的顺序不一致
 * 没有往集合中添加数据........
 */
public class MR_Join1 {
    static Configuration con;

    public static void main(String[] args) throws Exception {
        args = new String[]{"C:\\Users\\hp\\IdeaProjects\\GitHub_Maven\\src\\main\\resources\\jointask", "C:\\MAPREDYCE1"};
        con = new Configuration();
        Job job = Job.getInstance(con, "MR_Join1");
        job.setMapperClass(Map_Join1.class);
        job.setReducerClass(Reduce_Join1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean1.class);
        job.setOutputKeyClass(TableBean1.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.out.println(job.waitForCompletion(true) ? "zq" : "cw");
    }

    public static class Map_Join1 extends Mapper<LongWritable, Text, Text, TableBean1> {
        String name = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //获取读取的文件的名字
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            name = inputSplit.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //按行切
            TableBean1 tableBean = new TableBean1();
            Text text = new Text();
            String[] split = value.toString().split("\\s+");
            if (name.startsWith("order")) {
                tableBean.setID(Integer.parseInt(split[0]));
                tableBean.setPid(split[1]);
                tableBean.setAmount(Integer.parseInt(split[2]));
                tableBean.setPname(" ");
                tableBean.setFlog("order");
                text.set(split[1]);
            } else {
                tableBean.setID(0);
                tableBean.setPid(split[0]);
                tableBean.setAmount(0);
                tableBean.setPname(split[1]);
                tableBean.setFlog("pd");
                text.set(split[0]);
            }
            context.write(text, tableBean);
        }
    }

    public static class Reduce_Join1 extends Reducer<Text, TableBean1, TableBean1, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<TableBean1> values, Context context) throws IOException, InterruptedException {
            //存放订单表
            List<TableBean1> tableBean1s = new ArrayList<>();
            //存放pd表
            TableBean1 tableBeanPD = new TableBean1();
            for (TableBean1 value : values) {
                TableBean1 clone = null;
                if ("order".equals(value.getFlog())) {
                    try {
                        clone = (TableBean1) value.clone();
                    } catch (CloneNotSupportedException e) {
                        e.printStackTrace();
                    }
                    tableBean1s.add(clone);
                } else {
                    try {
                        tableBeanPD = (TableBean1) value.clone();
                    } catch (CloneNotSupportedException e) {
                        e.printStackTrace();
                    }
                }
            }
            for (TableBean1 tableBean1 : tableBean1s) {
                tableBean1.setPid(tableBeanPD.getPname());
                context.write(tableBean1, NullWritable.get());
            }
        }
    }
}
