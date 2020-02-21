package com.eph.automation.testing.models.dao.datalake;

public class LovTableDLObject {

    private String CODE;
    private String B_CLASSNAME;
    private int B_BATCHID;
    private String B_CREDATE;
    private String B_UPDDATE;
    private String B_CREATOR;
    private String B_UPDATOR;
    private String L_DESCRIPTION;
    private String L_START_DATE;
    private String L_END_DATE;

    public String getCODE() { return CODE; }
    public void setCODE(String CODE) {
        this.CODE = CODE;
    }

    public String getB_CLASSNAME() { return B_CLASSNAME; }
    public void setB_CLASSNAME(String B_CLASSNAME) {
        this.B_CLASSNAME = B_CLASSNAME;
    }

    public int getB_BATCHID() { return B_BATCHID; }
    public void setB_BATCHID(int B_BATCHID) {
        this.B_BATCHID = B_BATCHID;
    }

    public String getB_CREDATE() {
        return B_CREDATE;
    }
    public void setCREDATE(String B_CREDATE) {
        this.B_CREDATE = B_CREDATE;
    }

    public String getB_UPDDATE() {
        return B_UPDDATE;
    }
    public void setB_UPDDATE(String B_UPDDATE) {
        this.B_UPDDATE = B_UPDDATE;
    }

    public String getB_CREATOR() {
        return B_CREATOR;
    }
    public void setB_CREATOR(String B_CREATOR) {
        this.B_CREATOR = B_CREATOR;
    }

    public String getB_UPDATOR() { return B_UPDATOR; }
    public void setB_UPDATOR(String B_UPDATOR) { this.B_UPDATOR = B_UPDATOR; }

    public String getL_DESCRIPTION() { return L_DESCRIPTION; }
    public void setL_DESCRIPTION(String L_DESCRIPTION) { this.L_DESCRIPTION = L_DESCRIPTION; }

    public String getL_START_DATE() { return L_START_DATE; }
    public void setL_START_DATE(String L_START_DATE) { this.L_START_DATE = L_START_DATE; }

    public String getL_END_DATE() { return L_END_DATE; }
    public void setL_END_DATE(String L_END_DATE) { this.L_END_DATE = L_END_DATE; }

}
