package com.eph.automation.testing.services.talend;

import java.io.InputStream;
import java.net.URL;

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
                return TACJobConstants.JOB_110_EIP_SEMARCHY_KEY;
        }
    }

    public static int runPrintSummaryJob() {
        return TACServices.runTalendJobByEnv(
                TACJobConstants.CMXJobNames.PRINT_SUMMARY_REPORT_SEMARCHY_JOB.name(),
                TACJobConstants.EndPoints.SIT.toString());
    }
}
