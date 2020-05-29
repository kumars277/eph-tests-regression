package com.eph.automation.testing.models.dao.JRBIDataLakeAccess;

public class JRBIDLManifestationAccessObject {

    private String EPR;
    private String RECORD_TYPE;
    private String JOURNAL_PROD_SITE;
    private String JOURNAL_ISSUE_TRIM_SIZE;
    private String WAR_REFERENCE;
    private String DELTA_MODE;
    private String TYPE;

    public String getEPR() {
        return EPR;
    }
    public void setEPR(String EPR) {
        this.EPR = EPR;
    }

    public String getRECORD_TYPE() {
        return RECORD_TYPE;
    }
    public void setRECORD_TYPE(String RECORD_TYPE) {
        this.RECORD_TYPE = RECORD_TYPE;
    }

    public String getJOURNAL_PROD_SITE() {
        return JOURNAL_PROD_SITE;
    }
    public void setJOURNAL_PROD_SITE(String JOURNAL_PROD_SITE) {
        this.JOURNAL_PROD_SITE = JOURNAL_PROD_SITE;
    }

    public String getJOURNAL_ISSUE_TRIM_SIZE() {
        return JOURNAL_ISSUE_TRIM_SIZE;
    }
    public void setJOURNAL_ISSUE_TRIM_SIZE(String JOURNAL_ISSUE_TRIM_SIZE) {
        this.JOURNAL_ISSUE_TRIM_SIZE = JOURNAL_ISSUE_TRIM_SIZE;
    }

    public String getWAR_REFERENCE() {
        return WAR_REFERENCE;
    }
    public void setWAR_REFERENCE(String WAR_REFERENCE) {
        this.WAR_REFERENCE = WAR_REFERENCE;
    }

    public String getDELTA_MODE() {
        return DELTA_MODE;
    }
    public void setDELTA_MODE(String DELTA_MODE) {
        this.DELTA_MODE = DELTA_MODE;
    }

    public String getTYPE() {
        return TYPE;
    }
    public void setTYPE(String TYPE) {
        this.TYPE = TYPE;
    }

}

