package com.eph.automation.testing.services.db;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.contexts.LoadBatchContext;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;

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

    public static String createCustomerByStoreProcedure() {
        String batchID = null;
        Connection conn = DBManager.getPostgresConnection(Constants.EPH_SIT_URL);
        try {
            String query = "{? = call semarchy_repository.get_new_loadid(?, ?, ?,?)}";
            CallableStatement statement = conn.prepareCall(query);
            statement.registerOutParameter(1, Types.NUMERIC);
            //Need to confirm the following params with Ron
            statement.setString(2, "DATA_LOCATION_NAME");
            statement.setString(3, "TALEND");
            statement.setString(4, "FORCED_MERGE");
            statement.setString(6, "EPH Regression Test");
            statement.execute();

            String loadID = statement.getString(1);
            loadBatchContext.loadId = loadID;

            System.out.println(loadID);

            //Execute Insert Statements
            QueryRunner queryRunner = new QueryRunner();
            InsertEachEntities(loadID, queryRunner, conn);



            // Commit
            //Enabled Auto Commit
//            connection.commit();

            //Submit the Batch
            statement = conn.prepareCall("{? = call CMX_REPOSITORY.INTEGRATION_LOAD.SUBMIT_LOAD(?,?,?)}");
            statement.registerOutParameter(1, Types.NUMERIC);
            statement.setInt(2, Integer.valueOf(loadID));
            statement.setString(3, "ETL_ForcedMergeEcridMatchingJob");
            statement.setString(4, "EPH Regression Test");
            statement.execute();
            batchID = statement.getString(1);
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

    public static void InsertEachEntities(String loadID, QueryRunner queryRunner, Connection conn) throws SQLException {

        //Insert SQL to create each entities

        //
        //int updateStatus = queryRunner.update(conn, orgEntityInsertSQL.replace("VLOAD_ID", loadID));



    }


}
