package features.zOnHold.talend;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.LoadProperties;
import com.eph.automation.testing.helper.SFTPFunctions;
import features.zOnHold.znotused.context.DataLoadContext;
import com.eph.automation.testing.services.security.DecryptionService;

import java.io.InputStream;
import java.net.URL;
import java.util.Map;

/**
 * Created by RAVIVARMANS on 11/29/2018.
 */
public class TACServices {

    public static int runTalendJob(String jobName) {
        int inputStatus = Integer.MIN_VALUE;
        try {
            String jobHashKey = getJobHashKey(jobName);
            String jobCompletePath = TACJobConstants.EndPoints.SIT.toString() + jobHashKey;
            InputStream inputStream = new URL(jobCompletePath).openStream();
            inputStatus = inputStream.read();
            inputStream.close();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return inputStatus;
    }

    public static boolean uploadSourceFileAndRunTalendJob(DataLoadContext dataLoadContext) {
        try {
            //SFTP the Incremental File
            SFTPFunctions.SFTPUpload(dataLoadContext.incrementalFile, dataLoadContext.sftpLandingLocation);

            //Set the necessary Reference Parameters for Talend Job in My SQL
            Map contextMap = TalendUtils.getContextParamsForRinggoldIncremental();
            TalendUtils.performUpdate(dataLoadContext.TALEND_RINGGOLD_PROJECT_114_NAME, contextMap,
                    DecryptionService.decrypt(LoadProperties.getProperty(Constants.MYSQL_DB_URL_KEY)));

            //Kick Off the Talend Job
            int jobStatus = TACServices.runTalendJob(TACJobConstants.EPHJobNames.RINGGOLD_CMX_NEW_JOB.name());
            return jobStatus != Integer.MIN_VALUE;
        } catch (Exception e) {
            e.printStackTrace();
            return false;

        }
    }

    public static int runTalendJobByEnv(String jobName, String tacEnvironment) {
        int inputStatus = Integer.MIN_VALUE;
        try {
            String jobHashKey = getJobHashKey(jobName);
            String jobCompletePath = tacEnvironment + jobHashKey;
            InputStream inputStream = new URL(jobCompletePath).openStream();
            inputStatus = inputStream.read();
            inputStream.close();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return inputStatus;
    }

    private static String getJobHashKey(String jobName) {
        switch (jobName) {
            case "RINGGOLD_CMX_NEW_JOB":
                return TACJobConstants.JOB_200_PRJ_114_RINGGOLD_CMX_KEY;
            default:
                return TACJobConstants.JOB_YYY_PRJ_AAA_PRODUCT_EPH_JOB_KEY;
        }
    }


}
