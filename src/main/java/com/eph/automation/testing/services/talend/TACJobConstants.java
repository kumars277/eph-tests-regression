package com.eph.automation.testing.services.talend;

/**
 * Created by RAVIVARMANS on 11/29/2018.
 */
public class TACJobConstants {

    public static String JOB_200_PRJ_114_RINGGOLD_CMX_KEY = "ewoiYWN0aW9uTmFtZSI6InJ1blRhc2siLAoiYXV0aFBhc3MiOiAiT3V0NTZyc2IiLAoiYXV0aFVzZXIiOiAicmF2aXZhcm1hbnMiLAoibW9kZSI6ImFzeW5jaHJvbm91cyIsCiJ0YXNrSWQiOiIyMzUyIgp9";
    public static String JOB_YYY_PRJ_AAA_PRODUCT_EPH_JOB_KEY = "ewoiYWN0aW9uTmFtZSI6InJ1blRhc2siLAoiYXV0aFBhc3MiOiAiT3V0NTZyc2IiLAoiYXV0aFVzZXIiOiAicmF2aXZhcm1hbnMiLAoibW9kZSI6ImFzeW5jaHJvbm91cyIsCiJ0YXNrSWQiOiIyMzUyIgp9";

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

    public enum EPHJobNames {
        //Add the EPH Talend Jobs Name
        EPH_JOB_NAMES,
        RINGGOLD_CMX_NEW_JOB;
    }
}
