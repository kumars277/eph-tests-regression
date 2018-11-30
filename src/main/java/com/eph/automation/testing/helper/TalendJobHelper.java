package com.eph.automation.testing.helper;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.LoadProperties;
import com.eph.automation.testing.models.contexts.DataLoadContext;
import com.eph.automation.testing.services.security.DecryptionService;
import com.eph.automation.testing.services.talend.TACJobConstants;
import com.eph.automation.testing.services.talend.TACServices;
import com.eph.automation.testing.services.talend.TalendServerVariables;

import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * Created by RAVIVARMANS on 29/11/2018.
 */
public class TalendJobHelper implements TalendServerVariables {

    @StaticInjection
    public static DataLoadContext dataLoadContext;

    public static boolean runTalendJob() {
        try {
            //SFTP the Incremental File
            SFTPFunctions.SFTPUpload(dataLoadContext.incrementalFile, inputDirPrj114Job200);
            //Set the necessary Reference Parameters for Talend Job
            Map contextMap = TalendJobReferenceHelper.getContextParamsForRinggoldIncremental();
            TalendJobReferenceHelper.performUpdate(dataLoadContext.TALEND_PROJECT_008_NAME, contextMap,
                    DecryptionService.decrypt(LoadProperties.getProperty(Constants.MYSQL_SIT_DB_URL_KEY)));

            //Kick Off the Talend Job
            int jobStatus = TACServices.runTalendJob(TACJobConstants.EPHJobNames.RINGGOLD_CMX_NEW_JOB.name());
            assertTrue(jobStatus != Integer.MIN_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
            return false;

        }
        return true;
    }


}
