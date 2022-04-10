package com.atguigu.realtime.util;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/4/10 15:44
 */
public class JdbcUtils {

    /***
     * 查询hbas数据，返回
     * @param conn
     * @param sql
     * @param args  填充sql占位符
     * @param clazz 返回的结果泛型
     * @param flag 是否转成驼峰
     * @param <T>
     * @return
     */
    public static <T> List<T> queryList(
            Connection conn,
            String sql,
            Object[] args,
            Class<T> clazz,
            Boolean flag) throws IllegalAccessException, InstantiationException {

        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            //设置占位符
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }

            ResultSet resultSet = ps.executeQuery();
            List<T> list = new ArrayList<>();
            //将结果封装成反射对象T
            while (resultSet.next()) {      //hbase等外部数据库中查询出来的每一行结果
                T t = clazz.newInstance();
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                for (int i = 0; i < columnCount; i++) {     //处理每一行中的每一列
                    String columnName = metaData.getColumnName(i + 1);
                    Object value = resultSet.getObject(i + 1);
                    if (flag) {
                        //驼峰命名,com.google.guava包工具类
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    //commons-beanutils包下的工具类
                    BeanUtils.setProperty(t,columnName,value);
                }
                list.add(t);
            }
            return  list;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


}
