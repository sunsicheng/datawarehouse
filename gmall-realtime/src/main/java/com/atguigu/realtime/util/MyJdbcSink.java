package com.atguigu.realtime.util;

import com.atguigu.realtime.bean.OrderDetail;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/4/23 13:45
 */
public class MyJdbcSink {

    public static void main(String[] args) {
        getClickhouseSink("aa", "aa", OrderDetail.class);
    }

    public static <T> SinkFunction <T> getClickhouseSink(String db,
                                                             String table,
                                                             Class<T> clazz) {
        String url = "jdbc:clickhouse://192.168.100.162:8123/" + db;
        String driverName = "ru.yandex.clickhouse.ClickHouseDriver";
        //拼接sql insert into  table ( x,x,x,x ) values (?,?,?,?);
        StringBuilder builder = new StringBuilder();
        builder.append(" insert into ")
                .append(table)
                .append("( ");
        for (Field declaredField : clazz.getDeclaredFields()) {
            if (!Modifier.isTransient(declaredField.getModifiers())) {
                builder.append(declaredField.getName()).append(",");
            }
        }

        builder.deleteCharAt(builder.length() - 1);

        builder.append(" ) values ( ");

        for (Field declaredField : clazz.getDeclaredFields()) {
            if (!Modifier.isTransient(declaredField.getModifiers())) {
                builder.append("?,");
            }
        }

        builder.deleteCharAt(builder.length() - 1);
        builder.append(" )");

        System.out.println(builder);

        return getSink(builder.toString(), url, driverName);


    }


    public static <T> SinkFunction<T> getSink(String sql, String url, String driverName) {
        return JdbcSink.<T>sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        Field[] declaredFields = t.getClass().getDeclaredFields();
                        for (int i = 0,position=1; i < declaredFields.length; i++) {
                            if (!Modifier.isTransient(declaredFields[i].getModifiers())) {
                                try {
                                    //使用postion，避免因为有transit字段导致顺序错乱
                                    declaredFields[i].setAccessible(true);
                                    preparedStatement.setObject(position++,declaredFields[i].get(t));
                                } catch (IllegalAccessException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(url)
                        .withDriverName(driverName)
                        .build());
    }
}
