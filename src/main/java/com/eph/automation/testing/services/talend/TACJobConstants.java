package com.eph.automation.testing.services.talend;

/**
 * Created by RAVIVARMANS on 11/29/2018.
 */
public class TACJobConstants {

    public static String JOB_110_EIP_SEMARCHY_KEY = "ewoiYWN0aW9uTmFtZSI6InJ1blRhc2siLAoiYXV0aFBhc3MiOiAiT3V0NTVyc2IiLAoiYXV0aFVzZXIiOiAicmF2aXZhcm1hbnMiLAoibW9kZSI6ImFzeW5jaHJvbm91cyIsCiJ0YXNrSWQiOiIyMDU2Igp9";
    public static String JOB_200_PRJ_114_RINGGOLD_CMX_KEY = "ewoiYWN0aW9uTmFtZSI6InJ1blRhc2siLAoiYXV0aFBhc3MiOiAiT3V0NTVyc2IiLAoiYXV0aFVzZXIiOiAicmF2aXZhcm1hbnMiLAoibW9kZSI6ImFzeW5jaHJvbm91cyIsCiJ0YXNrSWQiOiIyMzUyIgp9";

    public enum EndPoints {
        DEV("http://tac.dev.ifp.elsevier.com:8080/org.talend.administrator/metaServlet?"),
        SIT("http://tac.sit.ifp.elsevier.com:8080/org.talend.administrator/metaServlet?"),
        UAT("http://tac.uat.ifp.elsevier.com:8080/org.talend.administrator/metaServlet?");

        EndPoints(String tacURL) {
            this.tacURL = tacURL;
        }

        private String tacURL;

        @Override
        public String toString() {
            return tacURL;
        }
    }

    public enum CMXJobNames {
        PRINT_SUMMARY_REPORT_SEMARCHY_JOB,
        AMPS_SEMARCHY_JOB,
        RINGGOLD_CMX_NEW_JOB;
    }
}
