package com.eph.automation.testing.models.dao.datalake;

import java.util.Objects;

/**
 * Created by RAVIVARMANS on 26/11/2018.
 */
public class WorkDataDLObject {


    private String WORK_ID;
    private String B_CLASSNAME;
    private String B_BATCHID;
    private String B_CREDATE;
    private String B_UPDDATE;
    private String B_CREATOR;
    private String B_UPDATOR;
    private String S_WORK_ID;
    private String EXTERNAL_REFERENCE;
    private String WORK_TITLE;
    private String S_WORK_TITLE;
    private String WORK_SUB_TITLE;
    private String S_WORK_SUB_TITLE;
    private String WORK_SHORT_TITLE;
    private String B_TOBATCHID;
    private String COPYRIGHT_YEAR;
    private String EDITION_NUMBER;
    private boolean T_SUMMARY_CHANGED;
    private String T_EVENT_DESCRIPTION;
    private String F_TYPE;
    private String F_STATUS;
    private String f_accountable_product;
    private String F_PMC;
    private String F_OA_TYPE;
    private String F_FAMILY;
    private String F_T_EVENT_TYPE;
    private String F_EVENT;
    private String F_SELF_ONE;
    private String F_SELF_TWO;
    private String F_SELF_THREE;
    private String F_SELF_FOUR;
    private String B_FROMBATCHID;
    private String F_SELF_EIGHT;
    private String F_SELF_SEVEN;
    private String F_SELF_SIX;
    private String F_SELF_FIVE;
    private String F_LLANGUAGE;
    private String F_SUBSCRIPTION_TYPE;
    private String F_SOCIETY_OWNERSHIP;
    private String F_IMPRINT;
    private String S_WORK_SHORT_TITLE;
    private boolean ELECTRO_RIGHTS_INDICATOR;
    private String VOLUME;
    private String WORK_FIN_ATTRIBS_ID;
    private String EFFECTIVE_START_DATE;
    private String EFFECTIVE_END_DATE;
    private String F_GL_COMPANY;
    private String F_GL_COST_RESP_CENTRE;
    private String F_GL_REVENUE_RESP_CENTRE;
    private String F_WWORK;
    private String WORK_IDENTIFIER_ID;
    private String IDENTIFIER;
    private String S_IDENTIFIER;

    public String getWORK_IDENTIFIER_ID() {   return WORK_IDENTIFIER_ID;  }
    public void setWORK_IDENTIFIER_ID(String WORK_IDENTIFIER_ID) {
        this.WORK_IDENTIFIER_ID = WORK_IDENTIFIER_ID;
    }

    public String getS_IDENTIFIER() {   return S_IDENTIFIER;  }
    public void setS_IDENTIFIER(String S_IDENTIFIER) {
        this.S_IDENTIFIER = S_IDENTIFIER;
    }

    public String getIDENTIFIER() { return IDENTIFIER;  }
    public void setIDENTIFIER(String IDENTIFIER) {
        this.IDENTIFIER = IDENTIFIER;
    }


    public String getF_WWORK() {
        return F_WWORK;
    }
    public void setF_WWORK(String F_WWORK) {
        this.F_WWORK = F_WWORK;
    }

    public String getF_GL_REVENUE_RESP_CENTRE() {
        return F_GL_REVENUE_RESP_CENTRE;
    }
    public void setF_GL_REVENUE_RESP_CENTRE(String F_GL_REVENUE_RESP_CENTRE) {
        this.F_GL_REVENUE_RESP_CENTRE = F_GL_REVENUE_RESP_CENTRE;
    }

    public String getF_GL_COST_RESP_CENTRE() {
        return F_GL_COST_RESP_CENTRE;
    }
    public void setF_GL_COST_RESP_CENTRE(String F_GL_COST_RESP_CENTRE) {
        this.F_GL_COST_RESP_CENTRE = F_GL_COST_RESP_CENTRE;
    }

    public String getF_GL_COMPANY() {
        return F_GL_COMPANY;
    }
    public void setF_GL_COMPANY(String F_GL_COMPANY) {
        this.F_GL_COMPANY = F_GL_COMPANY;
    }

    public String getEFFECTIVE_END_DATE() {
        return EFFECTIVE_END_DATE;
    }
    public void setEFFECTIVE_END_DATE(String EFFECTIVE_END_DATE) {
        this.EFFECTIVE_END_DATE = EFFECTIVE_END_DATE;
    }

    public String getEFFECTIVE_START_DATE() {
        return EFFECTIVE_START_DATE;
    }
    public void setEFFECTIVE_START_DATE(String EFFECTIVE_START_DATE) {
        this.EFFECTIVE_START_DATE = EFFECTIVE_START_DATE;
    }

    public String getWORK_FIN_ATTRIBS_ID() {
        return WORK_FIN_ATTRIBS_ID;
    }
    public void setWORK_FIN_ATTRIBS_ID(String WORK_FIN_ATTRIBS_ID) {
        this.WORK_FIN_ATTRIBS_ID = WORK_FIN_ATTRIBS_ID;
    }

    public String getWORK_ID() {
        return WORK_ID;
    }
    public void setWORK_ID(String WORK_ID) {
        this.WORK_ID = WORK_ID;
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

    public String getS_WORK_ID(){return S_WORK_ID;}
    public void setS_WORK_ID(String S_WORK_ID) { this.S_WORK_ID = S_WORK_ID;  }

    public String getEXTERNAL_REFERENCE() { return EXTERNAL_REFERENCE; }
    public void setEXTERNAL_REFERENCE(String EXTERNAL_REFERENCE) { this.EXTERNAL_REFERENCE = EXTERNAL_REFERENCE;  }

    public String getWORK_TITLE() { return WORK_TITLE;  }
    public void setWORK_TITLE(String WORK_TITLE) { this.WORK_TITLE = WORK_TITLE;  }

    public String getS_WORK_TITLE() { return S_WORK_TITLE;  }
    public void setS_WORK_TITLE(String S_WORK_TITLE) { this.S_WORK_TITLE = S_WORK_TITLE;  }

    public String getWORK_SUB_TITLE() { return WORK_SUB_TITLE;  }
    public void setWORK_SUB_TITLE(String WORK_SUB_TITLE) { this.WORK_SUB_TITLE = WORK_SUB_TITLE;  }

    public String getS_WORK_SUB_TITLE() { return S_WORK_SUB_TITLE;  }
    public void setS_WORK_SUB_TITLE(String S_WORK_SUB_TITLE) { this.S_WORK_SUB_TITLE = S_WORK_SUB_TITLE;  }

    public String getWORK_SHORT_TITLE() { return WORK_SHORT_TITLE;  }
    public void setWORK_SHORT_TITLE(String WORK_SHORT_TITLE) { this.WORK_SHORT_TITLE = WORK_SHORT_TITLE;  }

    public String getS_WORK_SHORT_TITLE() { return S_WORK_SHORT_TITLE;  }
    public void setS_WORK_SHORT_TITLE(String S_WORK_SHORT_TITLE) { this.S_WORK_SHORT_TITLE = S_WORK_SHORT_TITLE;  }

    public boolean getELECTRO_RIGHTS_INDICATOR() { return ELECTRO_RIGHTS_INDICATOR;  }
    public void setELECTRO_RIGHTS_INDICATOR(boolean ELECTRO_RIGHTS_INDICATOR) { this.ELECTRO_RIGHTS_INDICATOR = ELECTRO_RIGHTS_INDICATOR;  }

    public String getVOLUME() {      return VOLUME;   }
    public void setVOLUME(String VOLUME) {
        this.VOLUME = VOLUME;
    }

    public String getCOPYRIGHT_YEAR() {  return COPYRIGHT_YEAR;  }
    public void setCOPYRIGHT_YEAR(String COPYRIGHT_YEAR) {
        this.COPYRIGHT_YEAR = COPYRIGHT_YEAR;
    }

    public String getEDITION_NUMBER() { return EDITION_NUMBER; }
    public void setEDITION_NUMBER(String EDITION_NUMBER) {
        this.EDITION_NUMBER = EDITION_NUMBER;
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


    public String getF_PMC(){return F_PMC;}
    public void setF_PMC(String F_PMC) {
        this.F_PMC = F_PMC;
    }


    public String getF_OA_TYPE(){return F_OA_TYPE;}
    public void setF_OA_TYPE(String F_OA_TYPE) {
        this.F_OA_TYPE = F_OA_TYPE;
    }


    public String getF_FAMILY(){return F_FAMILY;}
    public void setF_FAMILY(String F_FAMILY) {
        this.F_FAMILY = F_FAMILY;
    }


    public String getF_IMPRINT(){return F_IMPRINT;}
    public void setF_IMPRINT(String F_IMPRINT) {
        this.F_IMPRINT = F_IMPRINT;
    }


    public String getF_SOCIETY_OWNERSHIP(){return F_SOCIETY_OWNERSHIP;}
    public void setF_SOCIETY_OWNERSHIP(String F_SOCIETY_OWNERSHIP) {
        this.F_SOCIETY_OWNERSHIP = F_SOCIETY_OWNERSHIP;
    }

    public String getF_SUBSCRIPTION_TYPE(){return F_SUBSCRIPTION_TYPE;}
    public void setF_SUBSCRIPTION_TYPE(String F_SUBSCRIPTION_TYPE) {
        this.F_SUBSCRIPTION_TYPE = F_SUBSCRIPTION_TYPE;
    }

    public String getF_LLANGUAGE(){return F_LLANGUAGE;}
    public void setF_LLANGUAGE(String F_LLANGUAGE) {
        this.F_LLANGUAGE = F_LLANGUAGE;
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

    public String getF_SELF_EIGHT() {  return F_SELF_EIGHT;  }
    public void setF_SELF_EIGHT(String F_SELF_EIGHT) {   this.F_SELF_EIGHT = F_SELF_EIGHT; }

    public String getB_FROMBATCHID() {  return B_FROMBATCHID;  }
    public void setB_FROMBATCHID(String B_FROMBATCHID) {   this.B_FROMBATCHID = B_FROMBATCHID; }

    public String getB_TOBATCHID() {  return B_TOBATCHID;  }
    public void setB_TOBATCHID(String B_TOBATCHID) {   this.B_TOBATCHID = B_TOBATCHID; }


}
