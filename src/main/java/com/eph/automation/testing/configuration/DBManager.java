package com.eph.automation.testing.configuration;

import com.eph.automation.testing.models.EnumConstants;
import com.eph.automation.testing.models.TestContext;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.StringUtils;
import org.knowm.yank.Yank;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/**
 * Created by RAVIVARMANS on 11/25/2018.
 */
public class DBManager {

    private static Connection connection = null;
    private static String driver = "oracle.jdbc.driver.OracleDriver";
    private static String mySQLDriver = "com.mysql.jdbc.Driver";

    public static List getDBResultAsBeanList(String sql, Class klass, String dbEndPoint) {
        Properties dbProps = new Properties();
        dbProps.setProperty("jdbcUrl", LoadProperties.getDBConnection(getDatabaseEnvironmentKey(dbEndPoint)));
        Yank.setupDataSource(dbProps);
        List klassList = Yank.queryBeanList(sql,klass, null);
        Yank.releaseDataSource();
        return klassList;
    }

    public static Connection getPostgresConnection(String dbEndPoint) {
        Properties dbProps = new Properties();
        dbProps.setProperty("jdbcUrl", LoadProperties.getDBConnection(getDatabaseEnvironmentKey(dbEndPoint)));
        if (connection == null) {
            try {
                connection = DriverManager.getConnection(LoadProperties.getDBConnection(dbEndPoint));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    public static String getDatabaseEnvironmentKey(String givenDBEndPoint) {
        String dbEndPointKey = givenDBEndPoint;
        if (EnumConstants.ENVIRONMENTS.CI.name().equalsIgnoreCase(TestContext.getValues().environment) && TestContext.getValues().targetDB) {
            //Get CI
        } else if (EnumConstants.ENVIRONMENTS.SIT.name().equalsIgnoreCase(TestContext.getValues().environment) && !TestContext.getValues().targetDB) {
            //Get SIT
        } else if (EnumConstants.ENVIRONMENTS.UAT.name().equalsIgnoreCase(TestContext.getValues().environment) && !TestContext.getValues().targetDB) {
            //GET UAT
            dbEndPointKey = Constants.PMX_URL;
        }
        return dbEndPointKey;
    }

    public static int mysqlConnection(String sql, Class classType, String URL) {
        int updateStatus = 0;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            if (connection == null) {
                DbUtils.loadDriver(mySQLDriver);
            }
            String host = StringUtils.substring(URL, 0, StringUtils.indexOf(URL, "?"));
            String user = StringUtils.substring(URL,StringUtils.indexOf(URL,"?")+1,StringUtils.indexOf(URL,"&")).replace("user=","").trim();
            String pwd = StringUtils.substring(URL,StringUtils.indexOf(URL,"&")+1).replace("password=","").trim();
            connection = DriverManager.getConnection(host,user,pwd);
            QueryRunner query = new QueryRunner();
            updateStatus = query.update(connection,sql);
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            DbUtils.closeQuietly(connection);
        }
        return updateStatus;
    }

    public static List getDBResultMap(String sql, String URL) {
        List mapList = null;
        try {
            if (connection == null) {
                DbUtils.loadDriver(driver);
            }
            connection = DriverManager.getConnection(LoadProperties.getDBConnection(URL));
            QueryRunner query = new QueryRunner();
            mapList = (List) query.query(connection, sql, new MapListHandler());
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        } finally {
            DbUtils.closeQuietly(connection);
        }
        return mapList;
    }

}
