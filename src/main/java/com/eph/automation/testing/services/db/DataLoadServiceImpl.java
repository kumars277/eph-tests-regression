package com.eph.automation.testing.services.db;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.configuration.LoadProperties;
import com.eph.automation.testing.models.contexts.LoadBatchContext;
import com.eph.automation.testing.models.contexts.NotificationCountContext;
import com.eph.automation.testing.services.db.sql.NegativeTestNotificationSQL;
import com.eph.automation.testing.services.db.sql.UpdateProductSQL;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Properties;

/**
 * Created by RAVIVARMANS on 28/11/2018.
 */
public class DataLoadServiceImpl {

    @StaticInjection
    public static LoadBatchContext loadBatchContext;

    private static Connection conn = null;

    public static String createProductByStoreProcedure() {
        String batchID = null;
        long batchId;
        BigDecimal loadId = null;
        int eventId;
        String query;
        conn=null;
        Properties dbProps = new Properties();
        dbProps.setProperty("jdbcUrl", LoadProperties.getDBConnection(DBManager.getDatabaseEnvironmentKey(Constants.EPH_URL)));
        if (conn == null) {
            try {
                conn = DriverManager.getConnection(LoadProperties.getDBConnection(Constants.EPH_URL));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        try {
            query = "{? = call semarchy_repository.get_new_loadid(?, ?, ?,?)}";
            CallableStatement statement = conn.prepareCall(query);
            statement.registerOutParameter(1, Types.DECIMAL);
            statement.setString(2, "EPH");
            statement.setString(3, "Manual");
            statement.setString(4, "Entity Load");
            statement.setString(5, "EPH Regression Test");
            statement.execute();

            loadId = statement.getBigDecimal(1);
            String loadID = String.valueOf(loadId);
            loadBatchContext.loadId = loadID;

            System.out.println(loadID);

//            //Need to gete New Event IDeventtype text, description text,
// thirdparty text, workflowid text, workflowsource text, loadid integer)
            //
            query = "{? = call semarchy_eph_mdm.eph_create_event(?,?,?,?,?,?)}";
            statement = conn.prepareCall(query);
            statement.registerOutParameter(1, Types.INTEGER);
            statement.setString(2, "NEW");
            statement.setString(3, "Regression test");
            statement.setString(4, null);
            statement.setString(5, "1");
            statement.setString(6, "TEST");
            statement.setInt(7, Integer.valueOf(loadID));
            statement.execute();
            eventId = statement.getInt(1);
            String eventID = String.valueOf(eventId);

            loadBatchContext.eventId = eventID;

            System.out.println(eventID);

            QueryRunner queryRunner = new QueryRunner();
            UpdateProduct(loadID, queryRunner, conn, eventID);
            statement = conn.prepareCall("{? = call semarchy_repository.SUBMIT_LOAD(?,?,?)}");
            statement.registerOutParameter(1, Types.BIGINT);
            statement.setLong(2, Long.valueOf(loadId.toString()));
            statement.setString(3, "Product");
            statement.setString(4, "EPH Regression Test");
            statement.execute();
            batchID = String.valueOf(statement.getLong(1));
            System.out.println(batchID);
            loadBatchContext.batchId = batchID;
            statement.close();
        } catch (SQLException exception) {
            exception.printStackTrace();
            DbUtils.rollbackAndCloseQuietly(conn);
        } finally {
            DbUtils.closeQuietly(conn);
        }


        return batchID;
    }

    public static void InsertEachEntities(QueryRunner queryRunner, Connection conn) throws SQLException {

        //Insert SQL to create each entities

        //Insert Product Line
        String saProductLineInserSQL = "INSERT INTO SA_PRODUCT_LINE(B_LOADID,B_CLASSNAME, PRODUCT_LINE_ID,F_TYPE,NAME,F_PARENT_PRODUCT_LINE) " +
                " VALUES (VLOAD_ID,'ProductLine','1000000','A','HS',null)";

        //Insert Subject Area
//        INSERT INTO SA_SUBJECT_AREA(B_LOADID,B_CLASSNAME,SUBJECT_AREA_ID,F_TYPE,CODE,NAME,F_PARENT_SUBJECT_AREA) VALUES (VLOAD_ID,'SubjectArea','1000000','A','1','Level1',null) ,(VLOAD_ID,'SubjectArea','1000001','A','2-a','Level2-a','1000000')

        //Insert Work
        String saProductWorkInsertSQL = "INSERT INTO semarchy_eph_mdm.SA_WWORK(B_LOADID,B_CLASSNAME,WORK_ID,WORK_TITLE,WORK_SUB_TITLE,F_TYPE,F_STATUS,WORK_KEY_TITLE,ELECTRO_RIGHTS_INDICATOR,VOLUME,COPYRIGHT_YEAR,F_ACCOUNTABLE_PRODUCT,F_PMC,F_OA_TYPE,F_FAMILY,F_IMPRINT,F_SOCIETY_OWNERSHIP,F_ACCESS_MODEL,F_EVENT) " +
                " VALUES  (" + loadBatchContext.loadId + ",'Work','8419','Notarzt-Leitfaden','Mit Zugang zur Medizinwelt','RBK','WDI',null,null,null,null,null,'689','N',null,'URBFI',null,null,"+loadBatchContext.eventId+")"
//                " VALUES  (" + loadBatchContext.loadId + ",'Work','3000','Manual of Critical Care Nursing','Nursing Interventions and Collaborative Management','TBK','WPU',null,null,null,'2015','3355','241','N',null,'MOSBY',null,null,"+loadBatchContext.eventId +")"
                ;

        System.out.println(saProductWorkInsertSQL);
        int updateStatus = queryRunner.update(conn, saProductWorkInsertSQL);

        System.out.println(updateStatus);



    }


    public static void UpdateProduct(String loadID, QueryRunner queryRunner, Connection conn, String eventID) throws SQLException {
        String updateProduct = "";
        int updateStatus;

        updateProduct = UpdateProductSQL.Insert_product;

        updateStatus = queryRunner.update(conn, updateProduct.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);

    }

    public static String createWorkByStoreProcedure() {
        String batchID = null;
        long batchId;
        BigDecimal loadId = null;
        int eventId;
        String query;
        conn=null;
        Properties dbProps = new Properties();
        dbProps.setProperty("jdbcUrl", LoadProperties.getDBConnection(DBManager.getDatabaseEnvironmentKey(Constants.EPH_URL)));
        if (conn == null) {
            try {
                conn = DriverManager.getConnection(LoadProperties.getDBConnection(Constants.EPH_URL));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        try {
            query = "{? = call semarchy_repository.get_new_loadid(?, ?, ?,?)}";
            CallableStatement statement = conn.prepareCall(query);
            statement.registerOutParameter(1, Types.DECIMAL);
            statement.setString(2, "EPH");
            statement.setString(3, "Manual");
            statement.setString(4, "Entity Load");
            statement.setString(5, "EPH Regression Test");
            statement.execute();

            loadId = statement.getBigDecimal(1);
            String loadID = String.valueOf(loadId);
            loadBatchContext.loadId = loadID;

            System.out.println(loadID);

//            //Need to gete New Event IDeventtype text, description text,
// thirdparty text, workflowid text, workflowsource text, loadid integer)
            //
            query = "{? = call semarchy_eph_mdm.eph_create_event(?,?,?,?,?,?)}";
            statement = conn.prepareCall(query);
            statement.registerOutParameter(1, Types.INTEGER);
            statement.setString(2, "NEW");
            statement.setString(3, "Regression test");
            statement.setString(4, null);
            statement.setString(5, "1");
            statement.setString(6, "TEST");
            statement.setInt(7, Integer.valueOf(loadID));
            statement.execute();
            eventId = statement.getInt(1);
            String eventID = String.valueOf(eventId);

            loadBatchContext.eventId = eventID;

            System.out.println(eventID);

            QueryRunner queryRunner = new QueryRunner();
            UpdateWork(loadID, queryRunner, conn, eventID);
            statement = conn.prepareCall("{? = call semarchy_repository.SUBMIT_LOAD(?,?,?)}");
            statement.registerOutParameter(1, Types.BIGINT);
            statement.setLong(2, Long.valueOf(loadId.toString()));
            statement.setString(3, "Product");
            statement.setString(4, "EPH Regression Test");
            statement.execute();
            batchID = String.valueOf(statement.getLong(1));
            System.out.println(batchID);
            loadBatchContext.batchId = batchID;
            statement.close();
        } catch (SQLException exception) {
            exception.printStackTrace();
            DbUtils.rollbackAndCloseQuietly(conn);
        } finally {
           DbUtils.closeQuietly(conn);
        }


        return batchID;
    }

    public static void UpdateWork(String loadID, QueryRunner queryRunner, Connection conn, String eventID) throws SQLException {
        String updateWork = "";
        int updateStatus;

        updateWork = UpdateProductSQL.Insert_work;

        updateStatus = queryRunner.update(conn, updateWork.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);

    }


    public static String createManifestationByStoreProcedure() {
        String batchID = null;
        long batchId;
        BigDecimal loadId = null;
        int eventId;
        String query;
        conn=null;
        Properties dbProps = new Properties();
        dbProps.setProperty("jdbcUrl", LoadProperties.getDBConnection(DBManager.getDatabaseEnvironmentKey(Constants.EPH_URL)));
        if (conn == null) {
            try {
                conn = DriverManager.getConnection(LoadProperties.getDBConnection(Constants.EPH_URL));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        try {
            query = "{? = call semarchy_repository.get_new_loadid(?, ?, ?,?)}";
            CallableStatement statement = conn.prepareCall(query);
            statement.registerOutParameter(1, Types.DECIMAL);
            statement.setString(2, "EPH");
            statement.setString(3, "Manual");
            statement.setString(4, "Entity Load");
            statement.setString(5, "EPH Regression Test");
            statement.execute();

            loadId = statement.getBigDecimal(1);
            String loadID = String.valueOf(loadId);
            loadBatchContext.loadId = loadID;

            System.out.println(loadID);

//            //Need to gete New Event IDeventtype text, description text,
// thirdparty text, workflowid text, workflowsource text, loadid integer)
            //
            query = "{? = call semarchy_eph_mdm.eph_create_event(?,?,?,?,?,?)}";
            statement = conn.prepareCall(query);
            statement.registerOutParameter(1, Types.INTEGER);
            statement.setString(2, "NEW");
            statement.setString(3, "Regression test");
            statement.setString(4, null);
            statement.setString(5, "1");
            statement.setString(6, "TEST");
            statement.setInt(7, Integer.valueOf(loadID));
            statement.execute();
            eventId = statement.getInt(1);
            String eventID = String.valueOf(eventId);

            loadBatchContext.eventId = eventID;

            System.out.println(eventID);

            QueryRunner queryRunner = new QueryRunner();
            UpdateManifestation(loadID, queryRunner, conn, eventID);
            statement = conn.prepareCall("{? = call semarchy_repository.SUBMIT_LOAD(?,?,?)}");
            statement.registerOutParameter(1, Types.BIGINT);
            statement.setLong(2, Long.valueOf(loadId.toString()));
            statement.setString(3, "Product");
            statement.setString(4, "EPH Regression Test");
            statement.execute();
            batchID = String.valueOf(statement.getLong(1));
            System.out.println(batchID);
            loadBatchContext.batchId = batchID;
            statement.close();
        } catch (SQLException exception) {
            exception.printStackTrace();
            DbUtils.rollbackAndCloseQuietly(conn);
        } finally {
            DbUtils.closeQuietly(conn);
        }


        return batchID;
    }

    public static void UpdateManifestation(String loadID, QueryRunner queryRunner, Connection conn, String eventID) throws SQLException {
        String updateManifestation = "";
        int updateStatus;

        updateManifestation = UpdateProductSQL.Insert_manifestation;

        updateStatus = queryRunner.update(conn, updateManifestation.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);

    }

    public static String createTestDataByStoreProcedure() {
        String batchID = null;
        long batchId;
        BigDecimal loadId = null;
        int eventId;
        String query;
        conn=null;
        Properties dbProps = new Properties();
        dbProps.setProperty("jdbcUrl", LoadProperties.getDBConnection(DBManager.getDatabaseEnvironmentKey(Constants.EPH_URL)));
        if (conn == null) {
            try {
                conn = DriverManager.getConnection(LoadProperties.getDBConnection(Constants.EPH_URL));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        try {
            query = "{? = call semarchy_repository.get_new_loadid(?, ?, ?,?)}";
            CallableStatement statement = conn.prepareCall(query);
            statement.registerOutParameter(1, Types.DECIMAL);
            statement.setString(2, "EPH");
            statement.setString(3, "Manual");
            statement.setString(4, "Entity Load");
            statement.setString(5, "EPH Regression Test");
            statement.execute();

            loadId = statement.getBigDecimal(1);
            String loadID = String.valueOf(loadId);
            loadBatchContext.loadId = loadID;

            System.out.println(loadID);

//            //Need to gete New Event IDeventtype text, description text,
// thirdparty text, workflowid text, workflowsource text, loadid integer)
            //
            query = "{? = call semarchy_eph_mdm.eph_create_event(?,?,?,?,?,?)}";
            statement = conn.prepareCall(query);
            statement.registerOutParameter(1, Types.INTEGER);
            statement.setString(2, "NEW");
            statement.setString(3, "Regression test");
            statement.setString(4, null);
            statement.setString(5, "1");
            statement.setString(6, "TEST");
            statement.setInt(7, Integer.valueOf(loadID));
            statement.execute();
            eventId = statement.getInt(1);
            String eventID = String.valueOf(eventId);

            loadBatchContext.eventId = eventID;

            System.out.println(eventID);

            QueryRunner queryRunner = new QueryRunner();
            CreateTestData(loadID, queryRunner, conn, eventID);
            statement = conn.prepareCall("{? = call semarchy_repository.SUBMIT_LOAD(?,?,?)}");
            statement.registerOutParameter(1, Types.BIGINT);
            statement.setLong(2, Long.valueOf(loadId.toString()));
            statement.setString(3, "Product");
            statement.setString(4, "EPH Regression Test");
            statement.execute();
            batchID = String.valueOf(statement.getLong(1));
            System.out.println(batchID);
            loadBatchContext.batchId = batchID;
            statement.close();
        } catch (SQLException exception) {
            exception.printStackTrace();
            DbUtils.rollbackAndCloseQuietly(conn);
        } finally {
            DbUtils.closeQuietly(conn);
        }


        return batchID;
    }

    public static void CreateTestData(String loadID, QueryRunner queryRunner, Connection conn, String eventID) throws SQLException {
        String updateManifestation = "";
        String updateManifestation1 = "";
        String updateWork = "";
        String updateProduct1 = "";
        String updateProduct2 = "";
        String updateProduct3 = "";
        String updateProduct4 = "";
        String updateProduct5 = "";
        String updateProduct6 = "";
        String updateProduct7 = "";
        int updateStatus;

        updateWork = UpdateProductSQL.Insert_work;
        updateManifestation = UpdateProductSQL.Insert_manifestation;
        updateManifestation1 = UpdateProductSQL.Insert_manifestation_1;
        updateProduct1 = UpdateProductSQL.Insert_product_1;
        updateProduct2 = UpdateProductSQL.Insert_product_2;
        updateProduct3 = UpdateProductSQL.Insert_product;
        updateProduct4 = UpdateProductSQL.Insert_product_4;
        updateProduct5 = UpdateProductSQL.Insert_product_5;
        updateProduct6 = UpdateProductSQL.Insert_product_6;
        updateProduct7 = UpdateProductSQL.Insert_product_7;

        updateStatus = queryRunner.update(conn, updateWork.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);
        updateStatus = queryRunner.update(conn, updateManifestation.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);
        updateStatus = queryRunner.update(conn, updateManifestation1.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);
        updateStatus = queryRunner.update(conn, updateProduct1.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);
        updateStatus = queryRunner.update(conn, updateProduct2.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);
        updateStatus = queryRunner.update(conn, updateProduct3.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);
        updateStatus = queryRunner.update(conn, updateProduct4.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);
        updateStatus = queryRunner.update(conn, updateProduct5.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);
        updateStatus = queryRunner.update(conn, updateProduct6.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);
        updateStatus = queryRunner.update(conn, updateProduct7.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);

    }

    // Negative Test 1
    public static String createFalseData1() {
        String batchID = null;
        long batchId;
        BigDecimal loadId = null;
        int eventId;
        String query;
        conn=null;
        Properties dbProps = new Properties();
        dbProps.setProperty("jdbcUrl", LoadProperties.getDBConnection(DBManager.getDatabaseEnvironmentKey(Constants.EPH_URL)));
        if (conn == null) {
            try {
                conn = DriverManager.getConnection(LoadProperties.getDBConnection(Constants.EPH_URL));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        try {
            query = "{? = call semarchy_repository.get_new_loadid(?, ?, ?,?)}";
            CallableStatement statement = conn.prepareCall(query);
            statement.registerOutParameter(1, Types.DECIMAL);
            statement.setString(2, "EPH");
            statement.setString(3, "Manual");
            statement.setString(4, "Entity Load");
            statement.setString(5, "EPH Regression Test");
            statement.execute();

            loadId = statement.getBigDecimal(1);
            String loadID = String.valueOf(loadId);
            loadBatchContext.loadId = loadID;

            System.out.println(loadID);

            query = "{? = call semarchy_eph_mdm.eph_create_event(?,?,?,?,?,?)}";
            statement = conn.prepareCall(query);
            statement.registerOutParameter(1, Types.INTEGER);
            statement.setString(2, "NEW");
            statement.setString(3, "Regression test");
            statement.setString(4, null);
            statement.setString(5, "1");
            statement.setString(6, "TEST");
            statement.setInt(7, Integer.valueOf(loadID));
            statement.execute();
            eventId = statement.getInt(1);
            String eventID = String.valueOf(eventId);

            loadBatchContext.eventId = eventID;

            System.out.println(eventID);

            QueryRunner queryRunner = new QueryRunner();
            insertFalseData(loadID, queryRunner, conn, eventID);
            statement = conn.prepareCall("{? = call semarchy_repository.SUBMIT_LOAD(?,?,?)}");
            statement.registerOutParameter(1, Types.BIGINT);
            statement.setLong(2, Long.valueOf(loadId.toString()));
            statement.setString(3, "Product");
            statement.setString(4, "EPH Regression Test");
            statement.execute();
            batchID = String.valueOf(statement.getLong(1));
            System.out.println(batchID);
            loadBatchContext.batchId = batchID;
            statement.close();
        } catch (SQLException exception) {
            exception.printStackTrace();
            DbUtils.rollbackAndCloseQuietly(conn);
        } finally {
            DbUtils.closeQuietly(conn);
        }


        return batchID;
    }

    public static void insertFalseData(String loadID, QueryRunner queryRunner, Connection conn, String eventID) throws SQLException {
        String updateManifestation = "";
        String updateManifestation1 = "";
        String updateWork = "";
        String updateProduct1 = "";
        String updateProduct2 = "";
        int updateStatus;

        updateWork = NegativeTestNotificationSQL.WORK_Negative_1;
        updateManifestation = NegativeTestNotificationSQL.Manifestation_1_Negative_1;
        updateManifestation1 = NegativeTestNotificationSQL.Manifestation_2_Negative_1;
        updateProduct1 = NegativeTestNotificationSQL.Product_1_Negative_1;
        updateProduct2 = NegativeTestNotificationSQL.Product_2_Negative_1;

        updateStatus = queryRunner.update(conn, updateWork.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);
        updateStatus = queryRunner.update(conn, updateManifestation.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);
        updateStatus = queryRunner.update(conn, updateManifestation1.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);
        updateStatus = queryRunner.update(conn, updateProduct1.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);
        updateStatus = queryRunner.update(conn, updateProduct2.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);

    }

    // Negative Test 2
    public static String createFalseData2() {
        String batchID = null;
        long batchId;
        BigDecimal loadId = null;
        int eventId;
        String query;
        conn=null;
        Properties dbProps = new Properties();
        dbProps.setProperty("jdbcUrl", LoadProperties.getDBConnection(DBManager.getDatabaseEnvironmentKey(Constants.EPH_URL)));
        if (conn == null) {
            try {
                conn = DriverManager.getConnection(LoadProperties.getDBConnection(Constants.EPH_URL));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        try {
            query = "{? = call semarchy_repository.get_new_loadid(?, ?, ?,?)}";
            CallableStatement statement = conn.prepareCall(query);
            statement.registerOutParameter(1, Types.DECIMAL);
            statement.setString(2, "EPH");
            statement.setString(3, "Manual");
            statement.setString(4, "Entity Load");
            statement.setString(5, "EPH Regression Test");
            statement.execute();

            loadId = statement.getBigDecimal(1);
            String loadID = String.valueOf(loadId);
            loadBatchContext.loadId = loadID;

            System.out.println(loadID);

            query = "{? = call semarchy_eph_mdm.eph_create_event(?,?,?,?,?,?)}";
            statement = conn.prepareCall(query);
            statement.registerOutParameter(1, Types.INTEGER);
            statement.setString(2, "NEW");
            statement.setString(3, "Regression test");
            statement.setString(4, null);
            statement.setString(5, "1");
            statement.setString(6, "TEST");
            statement.setInt(7, Integer.valueOf(loadID));
            statement.execute();
            eventId = statement.getInt(1);
            String eventID = String.valueOf(eventId);

            loadBatchContext.eventId = eventID;

            System.out.println(eventID);

            QueryRunner queryRunner = new QueryRunner();
            insertFalseData2(loadID, queryRunner, conn, eventID);
            statement = conn.prepareCall("{? = call semarchy_repository.SUBMIT_LOAD(?,?,?)}");
            statement.registerOutParameter(1, Types.BIGINT);
            statement.setLong(2, Long.valueOf(loadId.toString()));
            statement.setString(3, "Product");
            statement.setString(4, "EPH Regression Test");
            statement.execute();
            batchID = String.valueOf(statement.getLong(1));
            System.out.println(batchID);
            loadBatchContext.batchId = batchID;
            statement.close();
        } catch (SQLException exception) {
            exception.printStackTrace();
            DbUtils.rollbackAndCloseQuietly(conn);
        } finally {
            DbUtils.closeQuietly(conn);
        }


        return batchID;
    }

    public static void insertFalseData2(String loadID, QueryRunner queryRunner, Connection conn, String eventID) throws SQLException {
        String updateManifestation = "";
        String updateManifestation1 = "";
        String updateWork = "";
        String updateProduct1 = "";
        String updateProduct2 = "";
        int updateStatus;

        updateWork = NegativeTestNotificationSQL.WORK_Negative_2;
        updateManifestation = NegativeTestNotificationSQL.Manifestation_1_Negative_2;
        updateManifestation1 = NegativeTestNotificationSQL.Manifestation_2_Negative_2;
        updateProduct1 = NegativeTestNotificationSQL.Product_1_Negative_2;
        updateProduct2 = NegativeTestNotificationSQL.Product_2_Negative_2;

        updateStatus = queryRunner.update(conn, updateWork.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);
        updateStatus = queryRunner.update(conn, updateManifestation.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);
        updateStatus = queryRunner.update(conn, updateManifestation1.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);
        updateStatus = queryRunner.update(conn, updateProduct1.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);
        updateStatus = queryRunner.update(conn, updateProduct2.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);

    }

    // Negative Test 3
    public static String updateManifestation1() {
        String batchID = null;
        long batchId;
        BigDecimal loadId = null;
        int eventId;
        String query;
        conn=null;
        Properties dbProps = new Properties();
        dbProps.setProperty("jdbcUrl", LoadProperties.getDBConnection(DBManager.getDatabaseEnvironmentKey(Constants.EPH_URL)));
        if (conn == null) {
            try {
                conn = DriverManager.getConnection(LoadProperties.getDBConnection(Constants.EPH_URL));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        try {
            query = "{? = call semarchy_repository.get_new_loadid(?, ?, ?,?)}";
            CallableStatement statement = conn.prepareCall(query);
            statement.registerOutParameter(1, Types.DECIMAL);
            statement.setString(2, "EPH");
            statement.setString(3, "Manual");
            statement.setString(4, "Entity Load");
            statement.setString(5, "EPH Regression Test");
            statement.execute();

            loadId = statement.getBigDecimal(1);
            String loadID = String.valueOf(loadId);
            loadBatchContext.loadId = loadID;

            System.out.println(loadID);

            query = "{? = call semarchy_eph_mdm.eph_create_event(?,?,?,?,?,?)}";
            statement = conn.prepareCall(query);
            statement.registerOutParameter(1, Types.INTEGER);
            statement.setString(2, "NEW");
            statement.setString(3, "Regression test");
            statement.setString(4, null);
            statement.setString(5, "1");
            statement.setString(6, "TEST");
            statement.setInt(7, Integer.valueOf(loadID));
            statement.execute();
            eventId = statement.getInt(1);
            String eventID = String.valueOf(eventId);

            loadBatchContext.eventId = eventID;

            System.out.println(eventID);

            QueryRunner queryRunner = new QueryRunner();
            insertManifestation1(loadID, queryRunner, conn, eventID);
            statement = conn.prepareCall("{? = call semarchy_repository.SUBMIT_LOAD(?,?,?)}");
            statement.registerOutParameter(1, Types.BIGINT);
            statement.setLong(2, Long.valueOf(loadId.toString()));
            statement.setString(3, "Product");
            statement.setString(4, "EPH Regression Test");
            statement.execute();
            batchID = String.valueOf(statement.getLong(1));
            System.out.println(batchID);
            loadBatchContext.batchId = batchID;
            statement.close();
        } catch (SQLException exception) {
            exception.printStackTrace();
            DbUtils.rollbackAndCloseQuietly(conn);
        } finally {
            DbUtils.closeQuietly(conn);
        }


        return batchID;
    }

    public static void insertManifestation1(String loadID, QueryRunner queryRunner, Connection conn, String eventID) throws SQLException {
        String updateManifestation = "";
        int updateStatus;

        updateManifestation = NegativeTestNotificationSQL.Manifestation_2_Negative_1;

        updateStatus = queryRunner.update(conn, updateManifestation.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);
    }

    //Negative test 4
    public static String updateManifestation2() {
        String batchID = null;
        long batchId;
        BigDecimal loadId = null;
        int eventId;
        String query;
        conn=null;
        Properties dbProps = new Properties();
        dbProps.setProperty("jdbcUrl", LoadProperties.getDBConnection(DBManager.getDatabaseEnvironmentKey(Constants.EPH_URL)));
        if (conn == null) {
            try {
                conn = DriverManager.getConnection(LoadProperties.getDBConnection(Constants.EPH_URL));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        try {
            query = "{? = call semarchy_repository.get_new_loadid(?, ?, ?,?)}";
            CallableStatement statement = conn.prepareCall(query);
            statement.registerOutParameter(1, Types.DECIMAL);
            statement.setString(2, "EPH");
            statement.setString(3, "Manual");
            statement.setString(4, "Entity Load");
            statement.setString(5, "EPH Regression Test");
            statement.execute();

            loadId = statement.getBigDecimal(1);
            String loadID = String.valueOf(loadId);
            loadBatchContext.loadId = loadID;

            System.out.println(loadID);

            query = "{? = call semarchy_eph_mdm.eph_create_event(?,?,?,?,?,?)}";
            statement = conn.prepareCall(query);
            statement.registerOutParameter(1, Types.INTEGER);
            statement.setString(2, "NEW");
            statement.setString(3, "Regression test");
            statement.setString(4, null);
            statement.setString(5, "1");
            statement.setString(6, "TEST");
            statement.setInt(7, Integer.valueOf(loadID));
            statement.execute();
            eventId = statement.getInt(1);
            String eventID = String.valueOf(eventId);

            loadBatchContext.eventId = eventID;

            System.out.println(eventID);

            QueryRunner queryRunner = new QueryRunner();
            insertManifestation2(loadID, queryRunner, conn, eventID);
            statement = conn.prepareCall("{? = call semarchy_repository.SUBMIT_LOAD(?,?,?)}");
            statement.registerOutParameter(1, Types.BIGINT);
            statement.setLong(2, Long.valueOf(loadId.toString()));
            statement.setString(3, "Product");
            statement.setString(4, "EPH Regression Test");
            statement.execute();
            batchID = String.valueOf(statement.getLong(1));
            System.out.println(batchID);
            loadBatchContext.batchId = batchID;
            statement.close();
        } catch (SQLException exception) {
            exception.printStackTrace();
            DbUtils.rollbackAndCloseQuietly(conn);
        } finally {
            DbUtils.closeQuietly(conn);
        }


        return batchID;
    }


    public static void insertManifestation2(String loadID, QueryRunner queryRunner, Connection conn, String eventID) throws SQLException {
        String updateManifestation = "";
        int updateStatus;

        updateManifestation = NegativeTestNotificationSQL.Manifestation_1_Negative_2;

        updateStatus = queryRunner.update(conn, updateManifestation.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);
    }

    //Negative test 5
    public static String updateProduct1() {
        String batchID = null;
        long batchId;
        BigDecimal loadId = null;
        int eventId;
        String query;
        conn=null;
        Properties dbProps = new Properties();
        dbProps.setProperty("jdbcUrl", LoadProperties.getDBConnection(DBManager.getDatabaseEnvironmentKey(Constants.EPH_URL)));
        if (conn == null) {
            try {
                conn = DriverManager.getConnection(LoadProperties.getDBConnection(Constants.EPH_URL));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        try {
            query = "{? = call semarchy_repository.get_new_loadid(?, ?, ?,?)}";
            CallableStatement statement = conn.prepareCall(query);
            statement.registerOutParameter(1, Types.DECIMAL);
            statement.setString(2, "EPH");
            statement.setString(3, "Manual");
            statement.setString(4, "Entity Load");
            statement.setString(5, "EPH Regression Test");
            statement.execute();

            loadId = statement.getBigDecimal(1);
            String loadID = String.valueOf(loadId);
            loadBatchContext.loadId = loadID;

            System.out.println(loadID);

            query = "{? = call semarchy_eph_mdm.eph_create_event(?,?,?,?,?,?)}";
            statement = conn.prepareCall(query);
            statement.registerOutParameter(1, Types.INTEGER);
            statement.setString(2, "NEW");
            statement.setString(3, "Regression test");
            statement.setString(4, null);
            statement.setString(5, "1");
            statement.setString(6, "TEST");
            statement.setInt(7, Integer.valueOf(loadID));
            statement.execute();
            eventId = statement.getInt(1);
            String eventID = String.valueOf(eventId);

            loadBatchContext.eventId = eventID;

            System.out.println(eventID);

            QueryRunner queryRunner = new QueryRunner();
            insertProduct1(loadID, queryRunner, conn, eventID);
            statement = conn.prepareCall("{? = call semarchy_repository.SUBMIT_LOAD(?,?,?)}");
            statement.registerOutParameter(1, Types.BIGINT);
            statement.setLong(2, Long.valueOf(loadId.toString()));
            statement.setString(3, "Product");
            statement.setString(4, "EPH Regression Test");
            statement.execute();
            batchID = String.valueOf(statement.getLong(1));
            System.out.println(batchID);
            loadBatchContext.batchId = batchID;
            statement.close();
        } catch (SQLException exception) {
            exception.printStackTrace();
            DbUtils.rollbackAndCloseQuietly(conn);
        } finally {
            DbUtils.closeQuietly(conn);
        }


        return batchID;
    }


    public static void insertProduct1(String loadID, QueryRunner queryRunner, Connection conn, String eventID) throws SQLException {
        String updateProduct = "";
        int updateStatus;

        updateProduct = NegativeTestNotificationSQL.Product_1_Negative_1;

        updateStatus = queryRunner.update(conn, updateProduct.replace("LOADID", loadID).replace("EVENT", eventID));
        System.out.println(updateStatus);
    }

    public static String createIdentifier() {
        String batchID = null;
        long batchId;
        BigDecimal loadId = null;
        int eventId;
        String query;
        conn=null;
        Properties dbProps = new Properties();
        dbProps.setProperty("jdbcUrl", LoadProperties.getDBConnection(DBManager.getDatabaseEnvironmentKey(Constants.EPH_URL)));
        if (conn == null) {
            try {
                conn = DriverManager.getConnection(LoadProperties.getDBConnection(Constants.EPH_URL));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        try {
            query = "{? = call semarchy_repository.get_new_loadid(?, ?, ?,?)}";
            CallableStatement statement = conn.prepareCall(query);
            statement.registerOutParameter(1, Types.DECIMAL);
            statement.setString(2, "EPH");
            statement.setString(3, "Manual");
            statement.setString(4, "Entity Load");
            statement.setString(5, "EPH Regression Test");
            statement.execute();

            loadId = statement.getBigDecimal(1);
            String loadID = String.valueOf(loadId);
            loadBatchContext.loadId = loadID;

            System.out.println(loadID);

//            //Need to gete New Event IDeventtype text, description text,
// thirdparty text, workflowid text, workflowsource text, loadid integer)
            //
            query = "{? = call semarchy_eph_mdm.eph_create_event(?,?,?,?,?,?)}";
            statement = conn.prepareCall(query);
            statement.registerOutParameter(1, Types.INTEGER);
            statement.setString(2, "NEW");
            statement.setString(3, "Regression test");
            statement.setString(4, null);
            statement.setString(5, "1");
            statement.setString(6, "TEST");
            statement.setInt(7, Integer.valueOf(loadID));
            statement.execute();
            eventId = statement.getInt(1);
            String eventID = String.valueOf(eventId);

            loadBatchContext.eventId = eventID;

            System.out.println(eventID);

            QueryRunner queryRunner = new QueryRunner();
            InsertIdentifier(loadID, queryRunner, conn, eventID);
            statement = conn.prepareCall("{? = call semarchy_repository.SUBMIT_LOAD(?,?,?)}");
            statement.registerOutParameter(1, Types.BIGINT);
            statement.setLong(2, Long.valueOf(loadId.toString()));
            statement.setString(3, "Product");
            statement.setString(4, "EPH Regression Test");
            statement.execute();
            batchID = String.valueOf(statement.getLong(1));
            System.out.println(batchID);
            loadBatchContext.batchId = batchID;
            statement.close();
        } catch (SQLException exception) {
            exception.printStackTrace();
            DbUtils.rollbackAndCloseQuietly(conn);
        } finally {
            DbUtils.closeQuietly(conn);
        }


        return batchID;
    }

    public static void InsertIdentifier(String loadID, QueryRunner queryRunner, Connection conn, String eventID) throws SQLException {
        String updateIdentifier = "";
        int updateStatus;

        if(NotificationCountContext.manifestationIdentifier){
            updateIdentifier = UpdateProductSQL.Insert_manifestation_identifier;

            updateStatus = queryRunner.update(conn, updateIdentifier.replace("LOADID", loadID).replace("EVENT", eventID));
            System.out.println(updateStatus);
        }else {
            updateIdentifier = UpdateProductSQL.Insert_work_identifier;

            updateStatus = queryRunner.update(conn, updateIdentifier.replace("LOADID", loadID).replace("EVENT", eventID));
            System.out.println(updateStatus);
        }
    }
}
