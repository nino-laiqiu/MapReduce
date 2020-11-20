package com.tongyongtao.BigData.Hbase.HbaseDDL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: tongyongtao
 * Date: 2020-11-06
 * Time: 20:01
 */
public class HbaseUtils {
    private static Configuration configuration;
    private static Connection connection;


    static {

        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "linux03,linux04,linux05");
    }

    public static Connection getConnection() {
        // if (connection.isClosed() && connection== null)
            try {
                connection = ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                e.printStackTrace();
            }
        return connection;
    }


}
