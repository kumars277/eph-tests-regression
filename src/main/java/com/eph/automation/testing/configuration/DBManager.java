package com.eph.automation.testing.configuration;

import com.eph.automation.testing.models.EnumConstants;
import com.eph.automation.testing.models.TestContext;
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
            dbEndPointKey = Constants.PMX_UAT_URL;
        }
        return dbEndPointKey;
    }

}
