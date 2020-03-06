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

//  Table specific columns
    private String LEVEL_2_EVENT;
    private String LEVEL_3_EVENT;
    private String VALID_AT_WORK;
    private String VALID_AT_MANIFESTATION;
    private String VALID_AT_PRODUCT;
    private String VALID_FOR_BOOKS;
    private String VALID_FOR_JOURNALS;
    private String ROLL_UP_STATUS;
    private String ROLL_UP_TYPE;
    private String F_PMG;
    private String VALID_FOR_DIGITAL_PACKAGE;
    private String PARENT_DESCRIPTION;
    private String CHILD_DESCRIPTION;
    private String ROLL_UP_OWNERSHIP;


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

    public String getLEVEL_2_EVENT() { return LEVEL_2_EVENT; }
    public void setLEVEL_2_EVENT(String LEVEL_2_EVENT) {
        this.LEVEL_2_EVENT = LEVEL_2_EVENT;
    }

    public String getLEVEL_3_EVENT() { return LEVEL_3_EVENT; }
    public void setLEVEL_3_EVENT(String LEVEL_3_EVENT) {
        this.LEVEL_3_EVENT = LEVEL_3_EVENT;
    }

    public String getVALID_AT_WORK() { return VALID_AT_WORK; }
    public void setVALID_AT_WORK(String VALID_AT_WORK) {
        this.VALID_AT_WORK = VALID_AT_WORK;
    }

    public String getVALID_AT_MANIFESTATION() { return VALID_AT_MANIFESTATION; }
    public void setVALID_AT_MANIFESTATION(String VALID_AT_MANIFESTATION) { this.VALID_AT_MANIFESTATION = VALID_AT_MANIFESTATION; }

    public String getVALID_AT_PRODUCT() { return VALID_AT_PRODUCT; }
    public void setVALID_AT_PRODUCT(String VALID_AT_PRODUCT) {
        this.VALID_AT_PRODUCT = VALID_AT_PRODUCT;
    }

    public String getVALID_FOR_BOOKS() { return VALID_FOR_BOOKS; }
    public void setVALID_FOR_BOOKS(String VALID_FOR_BOOKS) {
        this.VALID_FOR_BOOKS = VALID_FOR_BOOKS;
    }

    public String getVALID_FOR_JOURNALS() { return VALID_FOR_JOURNALS; }
    public void setVALID_FOR_JOURNALS(String VALID_FOR_JOURNALS) {
        this.VALID_FOR_JOURNALS = VALID_FOR_JOURNALS;
    }

    public String getROLL_UP_STATUS() { return ROLL_UP_STATUS; }
    public void setROLL_UP_STATUS(String ROLL_UP_STATUS) {
        this.ROLL_UP_STATUS = ROLL_UP_STATUS;
    }

    public String getROLL_UP_TYPE() { return ROLL_UP_TYPE; }
    public void setROLL_UP_TYPE(String ROLL_UP_TYPE) { this.ROLL_UP_TYPE = ROLL_UP_TYPE; }

    public String getF_PMG() { return F_PMG; }
    public void setF_PMG(String F_PMG) { this.F_PMG = F_PMG; }

    public String getVALID_FOR_DIGITAL_PACKAGE() { return VALID_FOR_DIGITAL_PACKAGE; }
    public void setVALID_FOR_DIGITAL_PACKAGE(String VALID_FOR_DIGITAL_PACKAGE) { this.VALID_FOR_DIGITAL_PACKAGE = VALID_FOR_DIGITAL_PACKAGE; }

    public String getPARENT_DESCRIPTION() { return PARENT_DESCRIPTION; }
    public void setPARENT_DESCRIPTION(String PARENT_DESCRIPTION) { this.PARENT_DESCRIPTION = PARENT_DESCRIPTION; }

    public String getCHILD_DESCRIPTION() { return CHILD_DESCRIPTION; }
    public void setCHILD_DESCRIPTION(String CHILD_DESCRIPTION) { this.CHILD_DESCRIPTION = CHILD_DESCRIPTION; }

    public String getROLL_UP_OWNERSHIP() { return ROLL_UP_OWNERSHIP; }
    public void setROLL_UP_OWNERSHIP(String ROLL_UP_OWNERSHIP) { this.ROLL_UP_OWNERSHIP = ROLL_UP_OWNERSHIP; }

}
