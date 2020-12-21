package com.eph.automation.testing.configuration;

/**
 * Created by ravivarmans on 11/23/2018.
 */
public class RESTEndPoints {

    public enum PPM_CDS_API {
        UAT("http://uatbds.ppm-uat.tio.systems/api"),PROD("http://bds.ppm-prod.tio.systems/api");

        private String apiPath;

        PPM_CDS_API(String apiPath) {
            this.apiPath = apiPath;
        }

        @Override
        public String toString() {
            return apiPath;
        }
    }

    public enum ActivityAPI {
        DEV("http://10.183.67.153:8181"),SIT("http://10.183.67.153:8181");

        private String apiPath;

        ActivityAPI(String apiPath) {
            this.apiPath = apiPath;
        }

        @Override
        public String toString() {
            return apiPath;
        }
    }

    public enum Headers {
        Authorization("Basic YmJpX2FkbWluOmIxdHQzcnNoQG5keQ==");

        private String authorization;

        Headers(String authorization) {
            this.authorization = authorization;
        }

        @Override
        public String toString() {
            return authorization;
        }
    }

    public enum ActivityAuth {
        SIT_USER_NAME("jmfadmin_act"),
        SIT_PASS_WORD("")
        ;
        private String authorization;

        ActivityAuth(String authorization) {
            this.authorization = authorization;
        }

        @Override
        public String toString() {
            return authorization;
        }

    }
}
