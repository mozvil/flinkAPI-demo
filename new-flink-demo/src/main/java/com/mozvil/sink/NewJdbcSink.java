package com.mozvil.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class NewJdbcSink extends RichSinkFunction<String> {
	
	/**
	 * EXACTLY_ONCE version: https://www.yisu.com/zixun/55267.html
	 */

	private static final long serialVersionUID = -7671580395762451875L;
	
	private Connection connection = null;
    private PreparedStatement prepareStatement = null;
    
    @Override
    public void open(Configuration config) throws Exception {
        //Configuration globConf = null;
        //ParameterTool paras = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        //connection = DriverManager.getConnection(paras.get("mysql.url"), paras.get("mysql.username"), paras.get("mysql.password"));
        connection = DriverManager.getConnection("jdbc:mysql://192.168.20.100:3306/flink_test", "root", "12345678");
        String sql = "insert into t_eos values (?) on duplicate key update str = ?";
        prepareStatement = connection.prepareStatement(sql);
    }
    
    @Override
    public void invoke(String value, Context context) throws Exception {
        prepareStatement.setString(1, value);
        prepareStatement.setString(2, value);
        prepareStatement.execute();
    }
    
    @Override
    public void close() throws Exception {
        if (prepareStatement != null) {
            prepareStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

}
