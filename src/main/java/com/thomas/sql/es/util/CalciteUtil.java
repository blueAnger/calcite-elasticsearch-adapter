package com.thomas.sql.es.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.function.Function;

public class CalciteUtil
{
    public static <T> T execute(Properties properties, String sql, Function<ResultSet, T> function)
    {
        try(Connection connection = DriverManager.getConnection("jdbc:calcite:", properties)) {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            return function.apply(resultSet);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
