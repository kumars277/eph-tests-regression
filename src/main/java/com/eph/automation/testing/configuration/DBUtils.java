package com.eph.automation.testing.configuration;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.ColumnListHandler;
import org.apache.log4j.LogManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by RAVIVARMANS on 11/24/2018.
 */
public class DBUtils {

    private static org.apache.log4j.Logger LOGGER = LogManager.getLogger(DBUtils.class);

    public static <E> List<E> getDBResultAsEntity(String sql, String url, String username, String password, Class<E> classType) {
        List entityRows;
        try (
                Connection connection = DriverManager.getConnection(url, username, password)) {
            QueryRunner query = new QueryRunner();

            LOGGER.info("SQL Query : " + query);
            entityRows = (List) query.query(connection, sql, new BeanListHandler(classType));

        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            throw new RuntimeException(sqlException.getMessage(), sqlException);
        }
        return entityRows;
    }

    public static List<String> getResultsAsList(String sql, String url) {
        List entityRows;
        try (Connection connection = DriverManager.getConnection(url)) {
            QueryRunner query = new QueryRunner();
            LOGGER.info("SQL Query : " + query);
            entityRows = query.query(connection, sql, new ColumnListHandler<String>(1));
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            throw new RuntimeException(sqlException.getMessage(), sqlException);
        }
        return entityRows;
    }

}
