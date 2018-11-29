package com.eph.automation.testing.services.db.sql;

/**
 * Created by RAVIVARMANS on 29/11/2018.
 */
public class BatchJobChecksSQL {

    public static String UPDATE_TALEND_CONTEXT_VALUE = "update stalend_ref_01.tdmp_project_context_variables\n" +
            " set context_variable_value = 'PARAM1'\n" +
            " where project_name = 'PARAM2'\n" +
            " and context_variable_key = 'PARAM3'";

    public static String GET_BATCH_JOB_STATUS_FROM_MTA = "select count(*) AS JOBS_DONE from cmx_repository.MTA_JOB_LOG_PARAM JLP, cmx_repository.MTA_JOB_LOG JL where JLP.PARAM_NAME='V_BATCHID' \n" +
            " and JLP.PARAM_VALUE='PARAM1' and JLP.O_JOBLOG = JL.UUID and JL.STATUS IN ('DONE','SUSPENDED')";
}
