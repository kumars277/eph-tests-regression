package com.eph.automation.testing.models.dao.SDRMDataLake;

public class SDRMDLAccessObject {

    private String ISBN;
    private String TITLE;
    private String RENDITION_FORMAT;
    private String INBOUND_TS;
    private String PRODUCTION_DATE;
    private String EPR_ID;
    private String PRODUCT_TYPE;
    private String U_KEY;
    private String DELTA_MODE;

    public String getU_KEY() {
        return U_KEY;
    }
    public void setU_KEY(String U_KEY) {
        this.U_KEY = U_KEY;
    }

    public String getDELTA_MODE() {
        return DELTA_MODE;
    }
    public void setDELTA_MODE(String DELTA_MODE) {
        this.DELTA_MODE = DELTA_MODE;
    }


    public String getISBN() {
        return ISBN;
    }
    public void setISBN(String ISBN) {
        this.ISBN = ISBN;
    }

    public String getTITLE() {
        return TITLE;
    }
    public void setTITLE(String TITLE) {
        this.TITLE = TITLE;
    }

    public String getRENDITION_FORMAT() {
        return RENDITION_FORMAT;
    }
    public void setRENDITION_FORMAT(String RENDITION_FORMAT) {
        this.RENDITION_FORMAT = RENDITION_FORMAT;
    }

    public String getINBOUND_TS() {
        return INBOUND_TS;
    }
    public void setINBOUND_TS(String INBOUND_TS) {
        this.INBOUND_TS = INBOUND_TS;
    }

    public String getPRODUCTION_DATE() {
        return PRODUCTION_DATE;
    }
    public void setPRODUCTION_DATE(String PRODUCTION_DATE) {
        this.PRODUCTION_DATE = PRODUCTION_DATE;
    }

    public String getEPR_ID() {
        return EPR_ID;
    }
    public void setEPR_ID(String EPR_ID) {
        this.EPR_ID = EPR_ID;
    }

    public String getPRODUCT_TYPE() {
        return PRODUCT_TYPE;
    }
    public void setPRODUCT_TYPE(String PRODUCT_TYPE) {
        this.PRODUCT_TYPE = PRODUCT_TYPE;
    }



}
