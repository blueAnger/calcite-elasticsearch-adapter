package com.thomas.sql.es;

import com.thomas.sql.es.util.CalciteUtil;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

public class SQLTest
{
    private String[] sqlArray = {
            "select max(age) as maxAge, min(age) as minAge, count(\"name.raw\") as cnt from person",
            "select * from person order by age desc offset 2 rows fetch next 10 rows only"
    };

    private Properties properties = new Properties();

    @Before
    public void prepare()
    {
        String jsonPath = SQLTest.class.getResource("/my_es.json").toString();
        if(jsonPath.startsWith("file:"))
            jsonPath = jsonPath.substring("file:".length());

        properties.put("model", jsonPath);
        //in order to use '!=', otherwise we can just use '<>'
        properties.put("conformance", "ORACLE_10");
        //set this but useless. wonder why
        properties.put("caseSensitive", "false");
    }

    @Test
    public void testSelect() {
        long startTime = System.currentTimeMillis();
        CalciteUtil.execute(properties, sqlArray[1], (Function<ResultSet, Void>) resultSet -> {
            try {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int count = metaData.getColumnCount();
                while (resultSet.next())
                {
                    for(int i = 1; i <= count; ++i)
                        System.out.print(metaData.getColumnLabel(i) + ": " +
                                (resultSet.getObject(i) != null ? resultSet.getObject(i).toString() : "null") + " ");
                    System.out.println();
                }
                long endTime = System.currentTimeMillis();
                System.out.println("Time: " + (endTime - startTime) + " ms");
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        });
    }

    @Test
    public void testConcurrentSelect()
    {
        ExecutorService executorService = Executors.newFixedThreadPool(sqlArray.length);
        for(String sql : sqlArray)
            executorService.execute(new QueryRunnable(properties, sql));
        executorService.shutdown();
    }

    public static class QueryRunnable implements Runnable
    {
        private Properties properties;
        private String sql;

        public QueryRunnable(Properties properties, String sql)
        {
            this.properties = properties;
            this.sql = sql;
        }

        @Override
        public void run()
        {
            long startTime = System.currentTimeMillis();
            CalciteUtil.execute(properties, sql, (Function<ResultSet, Void>) resultSet -> {
                try {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int count = metaData.getColumnCount();
                    while (resultSet.next())
                    {
                        for(int i = 1; i <= count; ++i)
                            System.out.print(Thread.currentThread().getName() + " ===> " + metaData.getColumnLabel(i) + ": " +
                                    (resultSet.getObject(i) != null ? resultSet.getObject(i).toString() : "null") + " ");
                        System.out.println();
                    }
                    long endTime = System.currentTimeMillis();
                    System.out.println("Time: " + (endTime - startTime) + " ms");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            });
        }
    }

}
