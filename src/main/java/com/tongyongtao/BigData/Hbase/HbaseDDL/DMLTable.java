package com.tongyongtao.BigData.Hbase.HbaseDDL;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DMLTable {
    /**
     * Created with IntelliJ IDEA.
     * Description:
     * User: tongyongtao
     * Date: 2020-11-06
     * Time: 22:00
     */

    public static void main(String[] args) throws Exception {
        // putData("tab_2", "60005", "status", "name", "小胡");
        // getData("tab_2","10001");
        //  scanData("tab_2","","");
        // filterData("tab_2");
        // filterData1("tab_2","status","age","19");
        // filterData2("tab_2","status");
        filterData3("tab_2", "name");
    }

    public static void putData(String tableName, String rowkey, String cf, String cm, String values) throws IOException {
        Connection connection = HbaseUtils.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cm), Bytes.toBytes(values));
        table.put(put);
    }

    //获取数据get方法
    public static void getData(String tableName, String rowkey) throws IOException {
        Connection connection = HbaseUtils.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println(new String(CellUtil.cloneFamily(cell)));
            // System.out.println(Arrays.toString(Bytes.toBytes(ByteBuffer.wrap(CellUtil.cloneQualifier(cell)))));
            System.out.println(new String(CellUtil.cloneQualifier(cell)));
            System.out.println(new String(CellUtil.cloneValue(cell)));
            System.out.println(new String(CellUtil.cloneRow(cell)));
        }
    }

    //get方法获取数据
    public static void getData1(String tableName, String rowkey) throws IOException {
        Connection connection = HbaseUtils.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));

        List<Get> gets = new ArrayList<>();
        //获取多个rowkey
        gets.add(new Get(Bytes.toBytes("100001")));
        gets.add(new Get(Bytes.toBytes("10002")));
        Result[] results = table.get(gets);
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {

            }
        }
    }

    //scan方法
    public static void scanData(String tableName, String start, String stop) throws IOException {
        Connection connection = HbaseUtils.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    System.out.println(((Bytes.toString(CellUtil.cloneRow(cell)))));
                    System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
                }
            }
        }
    }

    //关于scan的过滤器  列族过滤
    public static void filterData(String tableName) throws IOException {
        Connection connection = HbaseUtils.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        Filter filter = new RowFilter(CompareOperator.EQUAL, new SubstringComparator("10001"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    //关于scan过滤  值过滤 一般采用SingleColumnValueFilter
    public static void filterData1(String tableName, String cf, String cm, String values) throws IOException {
        Connection connection = HbaseUtils.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        Filter filter = new SingleColumnValueFilter
                (Bytes.toBytes(cf), Bytes.toBytes(cm), CompareOperator.EQUAL, Bytes.toBytes(values));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    //列族的过滤
    public static void filterData2(String tableName, String cf) throws Exception {
        Connection connection = HbaseUtils.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        FamilyFilter filter = new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes(cf)));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    public static void filterData3(String tableName, String cm) throws IOException {
        Connection connection = HbaseUtils.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        QualifierFilter filter = new QualifierFilter(CompareOperator.EQUAL, new SubstringComparator(cm));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }
}