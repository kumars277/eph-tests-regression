package com.eph.automation.testing.configuration;

import com.eph.automation.testing.models.EnumConstants;
import com.eph.automation.testing.models.TestContext;
import com.eph.automation.testing.services.secure.EncryptOrDecryptService;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
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
    private static String PPM_PROD_ENCRYPTED_PASSWORD="eh10MvrWPRZd93nkR+pca0bRC6bzwXrT";
    private static String BBI_REDSHIFT_UAT_ENCRYPTED_PASSWORD="eh10MvrWPRZd93nkR+pca0bRC6bzwXrT";

    public static List getDBResultAsEntity(Class classType, String dbEndPoint, String sql) {
        List entityRows = null;
        try {
            if (connection == null) {
                DbUtils.loadDriver(driver);
            }
            connection = DriverManager.getConnection(LoadProperties.getDBConnection(getDatabaseEnvironmentKey(dbEndPoint)));
//            connection = DriverManager.getConnection(jdbcurl, username, pass);
            QueryRunner query = new QueryRunner();
            entityRows = (List) query.query(connection,sql,new BeanListHandler(classType));
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        } finally {
            DbUtils.closeQuietly(connection);
        }
        return entityRows;
    }

    public static List getDBResultAsBeanList(String sql, Class klass, Object[] params, String dbEndPoint) {
        Properties dbProps = new Properties();
        dbProps.setProperty("jdbcUrl", LoadProperties.getDBConnection(getDatabaseEnvironmentKey(dbEndPoint)));
        Yank.setupDataSource(dbProps);
        List klassList = Yank.queryBeanList(sql,klass, params);
        Yank.releaseDataSource();
        return klassList;
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


    public static List getPostgresDatabaseResults(String sql, Class klass) {
        Properties dbProps = new Properties();
        //jdbc:postgresql://localhost:5432/example", "postgres", "postgres"
        dbProps.setProperty("jdbcUrl", LoadProperties.getEndPoint(Constants.PPM_DEV_URL));
//        dbProps.setProperty("jdbcUrl", LoadProperties.getEndPoint(Constants.BBI_REDSHIFT_DEV_URL_KEY));
        dbProps.setProperty("username","bbi_master");
        dbProps.setProperty("password", EncryptOrDecryptService.decrypt(BBI_REDSHIFT_UAT_ENCRYPTED_PASSWORD));
//        dbProps.setProperty("password", EncryptOrDecryptService.decrypt(BBI_REDSHIFT_ENCRYPTED_PASSWORD));
        Yank.setupDataSource(dbProps);
        List klassList = Yank.queryBeanList(sql,klass, null);
        Yank.releaseDataSource();
        return klassList;
    }
}
