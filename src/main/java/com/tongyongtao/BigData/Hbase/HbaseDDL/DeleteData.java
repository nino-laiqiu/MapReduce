package com.tongyongtao.BigData.Hbase.HbaseDDL;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: tongyongtao
 * Date: 2020-11-07
 * Time: 14:41
 */
public class DeleteData {
    public static void main(String[] args) throws IOException {
        // deleteData("tab_2","10001");
        deleteData1("tab_2", "40001", "status");
    }

    //删除rowkey
    public static void deleteData(String tableName, String rowkey) throws IOException {
        Connection connection = HbaseUtils.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        table.delete(delete);
    }

    public static void deleteData1(String tableName, String rowkey, String cf) throws IOException {
        Table table = HbaseUtils.getConnection().getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        delete.addFamily(Bytes.toBytes(cf));
        table.delete(delete);
    }
}
