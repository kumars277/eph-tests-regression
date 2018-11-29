package com.eph.automation.testing.services.batch;


import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.batch.JobConfiguration;
import com.eph.automation.testing.models.contexts.LoadBatchContext;
import com.eph.automation.testing.services.db.sql.BatchJobChecksSQL;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * Created by RAVIVARMANS on 7/12/2018.
 */
public class JobStatusService {

    @StaticInjection
    public static LoadBatchContext loadBatchContext;

    public static boolean getJobStatusByBatchID() {
        String SQL = BatchJobChecksSQL.GET_BATCH_JOB_STATUS_FROM_MTA.replace("PARAM1",loadBatchContext.batchId);
//        System.out.println(SQL);
        List<JobConfiguration> jobStatus = DBManager.getDBResultAsBeanList(
                SQL, JobConfiguration.class, Constants.EPH_SIT_URL
        );
        if (jobStatus != null & jobStatus.size() > 0) {
            loadBatchContext.jobStatus = StringUtils.equalsIgnoreCase(jobStatus.get(0).Jobs_Done, "1") ? true:false;
//            System.out.println(loadBatchContext.jobStatus);
            return loadBatchContext.jobStatus;
        }
        return false;
    }
}
