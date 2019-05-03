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
        long batchId;
        BigDecimal loadId = null;
        int eventId;
        String query;
        Connection conn = DBManager.getPostgresConnection(Constants.EPH_URL);
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

//            //Need to get New Event IDeventtype text, description text,
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
            QueryRunner queryRunner = new QueryRunner();
            InsertEachEntities(queryRunner, conn);
//
//
//
//            // Commit
//            //Enabled Auto Commit
////            connection.commit();
//
//            //Submit the Batch
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


}
