package com.tongyongtao.BigData.Hbase.HbaseDDL;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: tongyongtao
 * Date: 2020-11-06
 * Time: 20:34
 * DDL语法
 */
public class DDLTable {
    public static void main(String[] args) throws IOException {
        // System.out.println(createTable("tab_111", "f1", "f2"));
        // System.out.println(isTableExist("tab_1"));
       // System.out.println(addFamily("tab_1", "f3"));
        //System.out.println(dropTable("tab_1"));
        System.out.println(createNamespace("myspace"));
    }

    //判断表是否存在
    public static Boolean isTableExist(String tableName) throws IOException {
        Connection connection = HbaseUtils.getConnection();
        //对表的操作
        Admin admin = connection.getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }

    public static Boolean createTable(String tableName, String... cfs) throws IOException {
        Connection connection = HbaseUtils.getConnection();
        //首先判断表是否存在
        //判断列族是否为空
        //发现一个错误,竟然没写!  查了好久
        if (!isTableExist(tableName)) {
            if (cfs.length > 0) {

                Admin admin = connection.getAdmin();
                //采用工厂方法
                TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
                //添加列族
                for (String cf : cfs) {
                    ColumnFamilyDescriptorBuilder columnFamily = ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes());
                    tableDescriptorBuilder.setColumnFamily(columnFamily.build());
                }
                admin.createTable(tableDescriptorBuilder.build());
                return true;
            }
        }
        return false;
    }

    public static boolean createTable1(String table, String... cfs) throws IOException {
        Connection connection = HbaseUtils.getConnection();
        //判断列族是否存在
        if (cfs == null) {
            return false;
        }
        //判断表是否存在
        if (isTableExist(table)) {
            return true;
        }
        Admin admin = connection.getAdmin();
        //表的构建
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(table));
        //列族的构建
        for (String cf : cfs) {
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes());
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }
        admin.createTable(tableDescriptorBuilder.build());
        return true;
    }
    //添加列族
    public static Boolean addFamily(String tableName, String... cfs) throws IOException {
        Connection connection = HbaseUtils.getConnection();
        Admin admin = connection.getAdmin();
        //先判断表是否存在
        if (isTableExist(tableName)) {
            for (String cf : cfs) {
                ColumnFamilyDescriptorBuilder columnFamily = ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes());
                admin.addColumnFamily(TableName.valueOf(tableName), columnFamily.build());
            }
            return true;
        }
        return false;
    }
    //删除表
    public static Boolean dropTable(String tableName) throws IOException {
        Connection connection = HbaseUtils.getConnection();
        if (isTableExist(tableName)) {

            Admin admin = connection.getAdmin();
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            return true;
        }
        return false;
    }
    //创建名称空间
    public  static  Boolean createNamespace(String spacename) throws IOException {
        Connection connection = HbaseUtils.getConnection();
        Admin admin = connection.getAdmin();
        NamespaceDescriptor build = NamespaceDescriptor.create(spacename).build();
        admin.createNamespace(build);
        return  true;
    }
}
