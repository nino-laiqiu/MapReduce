package com.tongyongtao.BigData.MapReduce.OutputformatCase;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: tongyongtao
 * Date: 2020-10-12
 * Time: 22:19
 * Hbase与MR的交互
 * 案例:电影案例 分析:设计rowkey  ImmutableBytesWritable
 */
public class MR_Hbase1 {
    public static void main(String[] args) throws Exception {
        args = new String[]{"C:\\Users\\hp\\IdeaProjects\\GitHub_Maven\\src\\main\\resources\\hbase.json"};
        Configuration con = HBaseConfiguration.create();
        con.set("hbase.zookeeper.quorum", "linux03,linux04,linux05");
        Job job = Job.getInstance(con, "MR_Hbase");
        job.setMapperClass(MAP_Hbase1.class);
        job.setReducerClass(Reduce_Hbase1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(HbaseBean1.class);
        // 穿件扫描对象用来扫描源hbase中的所有的数据
        Scan scan = new Scan();
        // 接收的扫描的数据的行数
        scan.setCaching(200);
        scan.setCacheBlocks(false);
        job.setJarByClass(HbaseBean1.class);

        // 输入数据的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 插入数据的表要存在
        TableMapReduceUtil.initTableReducerJob("movie1", Reduce_Hbase1.class, job);
        System.out.println(job.waitForCompletion(true) ? "zq" : "cw");
    }

    public static class MAP_Hbase1 extends Mapper<LongWritable, Text, Text, HbaseBean1> {
       static  Gson gson = new Gson();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                    //电影名字和时间龊构成一个rowkey
            HbaseBean1 hbaseBean = null;
            Text text = null;
            try {
                hbaseBean = gson.fromJson(value.toString(), HbaseBean1.class);
                String movie = StringUtils.leftPad(hbaseBean.getMovie(), 4, '0');
                String movieStamp = StringUtils.leftPad( String.valueOf(hbaseBean.getTimeStamp()),9,'0');
                text = new Text();
                text.set(movie+"_"+movieStamp);
            } catch (Exception e) {

            }
            context.write(text,hbaseBean);


        }
    }

    public static class Reduce_Hbase1 extends TableReducer<Text, HbaseBean1, ImmutableBytesWritable> {

        @Override
        protected void reduce(Text key, Iterable<HbaseBean1> values, Context context) throws IOException, InterruptedException {
            //key是rowkey values,迭代器只存储了一个
            Put put = new Put(key.toString().getBytes());
            for (HbaseBean1 value : values) {
                put.addColumn("cf".getBytes(),"moviename".getBytes(),value.getMovie().getBytes());
                put.addColumn("cf".getBytes(),"movierate".getBytes(),String.valueOf(value.getRate()).getBytes());
                put.addColumn("cf".getBytes(),"moviestamp".getBytes(),String.valueOf(value.getTimeStamp()).getBytes());
                put.addColumn("cf".getBytes(),"movieuid".getBytes(),value.getUid().getBytes());
            }
            context.write(null,put);


        }
    }
}
