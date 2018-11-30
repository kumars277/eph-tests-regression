package com.eph.automation.testing.services.db;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.contexts.LoadBatchContext;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Created by RAVIVARMANS on 28/11/2018.
 */
public class DataLoadServiceImpl {

    @StaticInjection
    public static LoadBatchContext loadBatchContext;

    public static String createProductByStoreProcedure() {
        String batchID = null;
        BigDecimal loadId = null;
        int eventId;
        String query;
        Connection conn = DBManager.getPostgresConnection(Constants.EPH_SIT_URL);
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
//
            System.out.println(eventID);
//
//
//            //Execute Insert Statements
//            QueryRunner queryRunner = new QueryRunner();
//            InsertEachEntities(loadID, queryRunner, conn);
//
//
//
//            // Commit
//            //Enabled Auto Commit
////            connection.commit();
//
//            //Submit the Batch
//            statement = conn.prepareCall("{? = call semarchy_repository.SUBMIT_LOAD(?,?,?)}");
//            statement.registerOutParameter(1, Types.DECIMAL);
//            statement.setBigDecimal(2, loadId);
//            statement.setString(3, "Entity Load");
//            statement.setString(4, "EPH Regression Test");
//            statement.execute();
//            batchID = statement.getString(1);
//            System.out.println(batchID);
//            loadBatchContext.batchId = batchID;
            statement.close();
        } catch (SQLException exception) {
            exception.printStackTrace();
            DbUtils.rollbackAndCloseQuietly(conn);
        } finally {
            DbUtils.closeQuietly(conn);
        }


        return batchID;
    }

    public static void InsertEachEntities(String loadID, QueryRunner queryRunner, Connection conn) throws SQLException {

        //Insert SQL to create each entities

        //Insert Product Line
        String saProductLineInserSQL = "INSERT INTO SA_PRODUCT_LINE(B_LOADID,B_CLASSNAME, PRODUCT_LINE_ID,F_TYPE,NAME,F_PARENT_PRODUCT_LINE) " +
                " VALUES (VLOAD_ID,'ProductLine','1000000','A','HS',null)";

        //Insert Subject Area
//        INSERT INTO SA_SUBJECT_AREA(B_LOADID,B_CLASSNAME,SUBJECT_AREA_ID,F_TYPE,CODE,NAME,F_PARENT_SUBJECT_AREA) VALUES (VLOAD_ID,'SubjectArea','1000000','A','1','Level1',null) ,(VLOAD_ID,'SubjectArea','1000001','A','2-a','Level2-a','1000000')

        //Insert Work
//        INSERT INTO SA_WWORK(B_LOADID,B_CLASSNAME,WORK_ID,WORK_TITLE,WORK_SUB_TITLE,F_TYPE,F_STATUS,WORK_KEY_TITLE,ELECTRO_RIGHTS_INDICATOR,VOLUME,COPYRIGHT_YEAR,F_ACCOUNTABLE_PRODUCT,F_PMC,F_OA_TYPE,F_FAMILY,F_IMPRINT,F_SOCIETY_OWNERSHIP,F_ACCESS_MODEL,F_EVENT) VALUES
//                (VLOAD_ID,'Work','3000','Manual of Critical Care Nursing','Nursing Interventions and Collaborative Management','TBK','WPU',null,null,null,'2015','3355','241','N',null,'MOSBY',null,null,VEVENT_ID))

        //int updateStatus = queryRunner.update(conn, saProductLineInserSQL.replace("VLOAD_ID", loadBatchContext.loadId));



    }


}
