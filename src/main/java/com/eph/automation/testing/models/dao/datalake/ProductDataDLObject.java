package com.eph.automation.testing.models.dao.datalake;

/**
 * Created by RAVIVARMANS on 26/11/2018.
 */
public class ProductDataDLObject {


    private String PRODUCT_ID;
    private String B_CLASSNAME;
    private String B_BATCHID;
    private String B_CREDATE;
    private String B_UPDDATE;
    private String B_CREATOR;
    private String B_UPDATOR;
    private String S_PRODUCT_ID;
    private String EXTERNAL_REFERENCE;
    private String NAME;
    private String S_NAME;
    private String SHORT_NAME;
    private String S_SHORT_NAME;
    private boolean SEPARATELY_SALE_INDICATOR;
    private boolean TRIAL_ALLOWED_INDICATOR;
    private String CONTENT_FROM_DATE;
    private String LAUNCH_DATE;
    private String CONTENT_TO_DATE;
    private String CONTENT_DATE_OFFSET;
    private boolean T_SUMMARY_CHANGED;
    private String T_EVENT_DESCRIPTION;
    private String F_TYPE;
    private String F_STATUS;
    private String f_accountable_product;
    private String F_TAX_CODE;
    private String F_REVENUE_MODEL;
    private String F_REVENUE_ACCOUNT;
    private String F_WWORK;
    private String F_MANIFESTATION;
    private String F_T_EVENT_TYPE;
    private String F_EVENT;
    private String F_SELF_ONE;
    private String F_SELF_TWO;
    private String F_SELF_THREE;
    private String F_SELF_FOUR;
    private String F_SELF_FIVE;
    private String F_SELF_SIX;
    private String F_SELF_SEVEN;
    private String B_FROMBATCHID;
    private String B_TOBATCHID;
    private String ACCOUNTABLE_PRODUCT_ID;
    private String GL_PRODUCT_SEGMENT_CODE;
    private String GL_PRODUCT_SEGMENT_NAME;
    private String F_GL_PRODUCT_SEGMENT_PARENT;
    private String PRODUCT_REL_PACK_ID;
    private String ALLOCATION;
    private String EFFECTIVE_START_DATE;
    private String EFFECTIVE_END_DATE;
    private String F_PACKAGE_OWNER;
    private String F_COMPONENT;
    private String F_RELATIONSHIP_TYPE;


    public String getF_RELATIONSHIP_TYPE() { return F_RELATIONSHIP_TYPE;  }
    public void setF_RELATIONSHIP_TYPE(String F_RELATIONSHIP_TYPE) {
        this.F_RELATIONSHIP_TYPE = F_RELATIONSHIP_TYPE;
    }

    public String getF_COMPONENT() { return F_COMPONENT;  }
    public void setF_COMPONENT(String F_COMPONENT) {
        this.F_COMPONENT = F_COMPONENT;
    }

    public String getEFFECTIVE_START_DATE() { return EFFECTIVE_START_DATE;  }
    public void setEFFECTIVE_START_DATE(String EFFECTIVE_START_DATE) {
        this.EFFECTIVE_START_DATE = EFFECTIVE_START_DATE;
    }

    public String getF_PACKAGE_OWNER() { return F_PACKAGE_OWNER;  }
    public void setF_PACKAGE_OWNER(String F_PACKAGE_OWNER) {
        this.F_PACKAGE_OWNER = F_PACKAGE_OWNER;
    }

    public String getEFFECTIVE_END_DATE() { return EFFECTIVE_END_DATE;  }
    public void setEFFECTIVE_END_DATE(String EFFECTIVE_END_DATE) {
        this.EFFECTIVE_END_DATE = EFFECTIVE_END_DATE;
    }

    public String getPRODUCT_REL_PACK_ID() { return PRODUCT_REL_PACK_ID;  }
    public void setPRODUCT_REL_PACK_ID(String PRODUCT_REL_PACK_ID) {
        this.PRODUCT_REL_PACK_ID = PRODUCT_REL_PACK_ID;
    }

    public String getALLOCATION() { return ALLOCATION;  }
    public void setALLOCATION(String ALLOCATION) {
        this.ALLOCATION = ALLOCATION;
    }

    public String getPRODUCT_ID() { return PRODUCT_ID;  }
    public void setPRODUCT_ID(String PRODUCT_ID) {
        this.PRODUCT_ID = PRODUCT_ID;
    }

    public String getB_CLASSNAME() {
        return B_CLASSNAME;
    }
    public void setB_CLASSNAME(String b_CLASSNAME) { this.B_CLASSNAME = b_CLASSNAME;  }

    public String getB_BATCHID() {
        return B_BATCHID;
    }
    public void setB_BATCHID(String B_BATCHID) { this.B_BATCHID = B_BATCHID;  }

    public String getB_CREDATE(){return B_CREDATE;}
    public void setB_CREDATE(String B_CREDATE) { this.B_CREDATE = B_CREDATE;  }

    public String getB_UPDDATE(){return B_UPDDATE;}
    public void setB_UPDDATE(String B_UPDDATE) { this.B_UPDDATE = B_UPDDATE;  }

    public String getB_CREATOR(){return B_CREATOR;}
    public void setB_CREATOR(String B_CREATOR) { this.B_CREATOR = B_CREATOR;  }

    public String getB_UPDATOR(){return B_UPDATOR;}
    public void setB_UPDATOR(String B_UPDATOR) { this.B_UPDATOR = B_UPDATOR;  }

    public String getS_PRODUCT_ID(){return S_PRODUCT_ID;}
    public void setS_PRODUCT_ID(String S_PRODUCT_ID) { this.S_PRODUCT_ID = S_PRODUCT_ID;  }

    public String getEXTERNAL_REFERENCE() { return EXTERNAL_REFERENCE; }
    public void setEXTERNAL_REFERENCE(String EXTERNAL_REFERENCE) { this.EXTERNAL_REFERENCE = EXTERNAL_REFERENCE;  }

    public String getNAME() { return NAME;  }
    public void setNAME(String NAME) { this.NAME = NAME;  }

    public String getS_NAME() { return S_NAME;  }
    public void setS_NAME(String S_NAME) { this.S_NAME = S_NAME;  }

    public String getSHORT_NAME() { return SHORT_NAME;  }
    public void setSHORT_NAME(String SHORT_NAME) { this.SHORT_NAME = SHORT_NAME;  }


    public String getS_SHORT_NAME() { return S_SHORT_NAME;  }
    public void setS_SHORT_NAME(String S_SHORT_NAME) { this.S_SHORT_NAME = S_SHORT_NAME;  }



    public boolean getSEPARATELY_SALE_INDICATOR() { return SEPARATELY_SALE_INDICATOR;  }
    public void setSEPARATELY_SALE_INDICATOR(boolean SEPARATELY_SALE_INDICATOR) { this.SEPARATELY_SALE_INDICATOR = SEPARATELY_SALE_INDICATOR;  }


    public boolean getTRIAL_ALLOWED_INDICATOR() { return TRIAL_ALLOWED_INDICATOR;  }
    public void setTRIAL_ALLOWED_INDICATOR(boolean TRIAL_ALLOWED_INDICATOR) { this.TRIAL_ALLOWED_INDICATOR = TRIAL_ALLOWED_INDICATOR;  }


    public String getLAUNCH_DATE() {      return LAUNCH_DATE;   }
    public void setLAUNCH_DATE(String LAUNCH_DATE) {
        this.LAUNCH_DATE = LAUNCH_DATE;
    }

    public String getCONTENT_FROM_DATE() {  return CONTENT_FROM_DATE;  }
    public void setCONTENT_FROM_DATE(String CONTENT_FROM_DATE) {
        this.CONTENT_FROM_DATE = CONTENT_FROM_DATE;
    }


    public String getCONTENT_TO_DATE() { return CONTENT_TO_DATE; }
    public void setCONTENT_TO_DATE(String CONTENT_TO_DATE) {
        this.CONTENT_TO_DATE = CONTENT_TO_DATE;
    }


    public String getCONTENT_DATE_OFFSET() { return CONTENT_DATE_OFFSET; }
    public void setCONTENT_DATE_OFFSET(String CONTENT_DATE_OFFSET) {
        this.CONTENT_DATE_OFFSET = CONTENT_DATE_OFFSET;
    }


    public boolean getT_SUMMARY_CHANGED() { return T_SUMMARY_CHANGED; }
    public void setT_SUMMARY_CHANGED(boolean T_SUMMARY_CHANGED) {
        this.T_SUMMARY_CHANGED = T_SUMMARY_CHANGED;
    }


    public String getT_EVENT_DESCRIPTION() { return T_EVENT_DESCRIPTION; }
    public void setT_EVENT_DESCRIPTION(String  T_EVENT_DESCRIPTION) {
        this.T_EVENT_DESCRIPTION = T_EVENT_DESCRIPTION;
    }


    public String getF_TYPE() { return F_TYPE; }
    public void setF_TYPE(String f_TYPE) {
        F_TYPE = f_TYPE;
    }

    public String getF_STATUS() {  return F_STATUS;  }
    public void setF_STATUS(String f_STATUS) {
        F_STATUS = f_STATUS;
    }

    public String getf_accountable_product() {  return f_accountable_product;   }
    public void setF_accountable_product(String f_accountable_product) {    this.f_accountable_product = f_accountable_product;   }


    public String getF_TAX_CODE(){return F_TAX_CODE;}
    public void setF_TAX_CODE(String F_TAX_CODE) {
        this.F_TAX_CODE = F_TAX_CODE;
    }

    public String getF_REVENUE_MODEL(){return F_REVENUE_MODEL;}
    public void setF_REVENUE_MODEL(String F_REVENUE_MODEL) {
        this.F_REVENUE_MODEL = F_REVENUE_MODEL;
    }

    public String getF_REVENUE_ACCOUNT(){return F_REVENUE_ACCOUNT;}
    public void setF_REVENUE_ACCOUNT(String F_REVENUE_ACCOUNT) {
        this.F_REVENUE_ACCOUNT = F_REVENUE_ACCOUNT;
    }

    public String getF_WWORK(){return F_WWORK;}
    public void setF_WWORK(String F_WWORK) {
        this.F_WWORK = F_WWORK;
    }

    public String getF_MANIFESTATION(){return F_MANIFESTATION;}
    public void setF_MANIFESTATION(String F_MANIFESTATION) {
        this.F_MANIFESTATION = F_MANIFESTATION;
    }

    public String getF_T_EVENT_TYPE(){return F_T_EVENT_TYPE;}
    public void setF_T_EVENT_TYPE(String F_T_EVENT_TYPE) {
        this.F_T_EVENT_TYPE = F_T_EVENT_TYPE;
    }

    public String getF_EVENT() {  return F_EVENT;  }
    public void setF_EVENT(String f_EVENT) {
        F_EVENT = f_EVENT;
    }

    public String getF_SELF_ONE() {  return F_SELF_ONE;  }
    public void setF_SELF_ONE(String F_SELF_ONE) {
        this.F_SELF_ONE = F_SELF_ONE;
    }

    public String getF_SELF_TWO() {  return F_SELF_TWO;  }
    public void setF_SELF_TWO(String F_SELF_TWO) {
        this.F_SELF_TWO = F_SELF_TWO;
    }

    public String getF_SELF_THREE() {  return F_SELF_THREE;  }
    public void setF_SELF_THREE(String F_SELF_THREE) {
        this.F_SELF_THREE = F_SELF_THREE;
    }

    public String getF_SELF_FOUR() {  return F_SELF_FOUR;  }
    public void setF_SELF_FOUR(String F_SELF_FOUR) {
        this.F_SELF_FOUR = F_SELF_FOUR;
    }

    public String getF_SELF_FIVE() {  return F_SELF_FIVE;  }
    public void setF_SELF_FIVE(String F_SELF_FIVE) { this.F_SELF_FIVE = F_SELF_FIVE;  }

    public String getF_SELF_SIX() {  return F_SELF_SIX;  }
    public void setF_SELF_SIX(String F_SELF_SIX) {
        this.F_SELF_SIX = F_SELF_SIX;
    }



    public String getF_SELF_SEVEN() {  return F_SELF_SEVEN;  }
    public void setF_SELF_SEVEN(String F_SELF_SEVEN) {
        this.F_SELF_SEVEN = F_SELF_SEVEN;
    }


    public String getB_FROMBATCHID() {  return B_FROMBATCHID;  }
    public void setB_FROMBATCHID(String B_FROMBATCHID) {
        this.B_FROMBATCHID = B_FROMBATCHID;
    }


    public String getB_TOBATCHID() {  return B_TOBATCHID;  }
    public void setB_TOBATCHID(String B_TOBATCHID) {
        this.B_TOBATCHID = B_TOBATCHID;
    }


    public String getACCOUNTABLE_PRODUCT_ID() {return ACCOUNTABLE_PRODUCT_ID; }
    public void setACCOUNTABLE_PRODUCT_ID(String ACCOUNTABLE_PRODUCT_ID) {
        this.ACCOUNTABLE_PRODUCT_ID = ACCOUNTABLE_PRODUCT_ID;
    }


    public String getGL_PRODUCT_SEGMENT_CODE() {return GL_PRODUCT_SEGMENT_CODE; }
    public void setGL_PRODUCT_SEGMENT_CODE(String GL_PRODUCT_SEGMENT_CODE) {
        this.GL_PRODUCT_SEGMENT_CODE = GL_PRODUCT_SEGMENT_CODE;
    }


    public String getGL_PRODUCT_SEGMENT_NAME() {return GL_PRODUCT_SEGMENT_NAME; }
    public void setGL_PRODUCT_SEGMENT_NAME(String GL_PRODUCT_SEGMENT_NAME) {
        this.GL_PRODUCT_SEGMENT_NAME = GL_PRODUCT_SEGMENT_NAME;
    }


    public String getF_GL_PRODUCT_SEGMENT_PARENT() {return F_GL_PRODUCT_SEGMENT_PARENT; }
    public void setF_GL_PRODUCT_SEGMENT_PARENT(String F_GL_PRODUCT_SEGMENT_PARENT) {
        this.F_GL_PRODUCT_SEGMENT_PARENT = F_GL_PRODUCT_SEGMENT_PARENT;
    }





}
