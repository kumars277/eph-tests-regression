package com.eph.automation.testing.models.dao.EPHDataLake;

/**
 * Created by RAVIVARMANS on 26/11/2018.
 */
public class ManifestationDataDLObject {


    private String MANIFESTATION_ID;
    private String B_CLASSNAME;
    private String B_BATCHID;
    private String B_CREDATE;
    private String B_UPDDATE;
    private String B_CREATOR;
    private String B_UPDATOR;
    private String S_MANIFESTATION_ID;
    private String EXTERNAL_REFERENCE;
    private String MANIFESTATION_KEY_TITLE;
    private String S_MANIFESTATION_KEY_TITLE;
    private boolean INTER_EDITION_FLAG;
    private String FIRST_PUB_DATE;
    private String LAST_PUB_DATE;
    private String T_EVENT_DESCRIPTION;
    private String F_TYPE;
    private String F_STATUS;
    private String F_FORMAT_TYPE;
    private String F_WWORK;
    private String F_T_EVENT_TYPE;
    private String F_EVENT;
    private String F_SELF_ONE;
    private String B_FROMBATCHID;
    private String B_TOBATCHID;
    private String MANIF_IDENTIFIER_ID;
    private String EFFECTIVE_START_DATE;
    private String EFFECTIVE_END_DATE;
    private String F_MANIFESTATION;

    public String getMANIFESTATION_ID() {
        return MANIFESTATION_ID;
    }
    public void setMANIFESTATION_ID(String MANIFESTATION_ID) {
        this.MANIFESTATION_ID = MANIFESTATION_ID;
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

    public String getS_MANIFESTATION_ID(){return S_MANIFESTATION_ID;}
    public void setS_MANIFESTATION_ID(String S_MANIFESTATION_ID) { this.S_MANIFESTATION_ID = S_MANIFESTATION_ID;  }

    public String getEXTERNAL_REFERENCE() { return EXTERNAL_REFERENCE; }
    public void setEXTERNAL_REFERENCE(String EXTERNAL_REFERENCE) { this.EXTERNAL_REFERENCE = EXTERNAL_REFERENCE;  }

    public String getMANIFESTATION_KEY_TITLE() { return MANIFESTATION_KEY_TITLE;  }
    public void setMANIFESTATION_KEY_TITLE(String MANIFESTATION_KEY_TITLE) { this.MANIFESTATION_KEY_TITLE = MANIFESTATION_KEY_TITLE;  }


    public String getS_MANIFESTATION_KEY_TITLE() { return S_MANIFESTATION_KEY_TITLE;  }
    public void setS_MANIFESTATION_KEY_TITLE(String S_MANIFESTATION_KEY_TITLE) { this.S_MANIFESTATION_KEY_TITLE = S_MANIFESTATION_KEY_TITLE;  }

    public boolean getINTER_EDITION_FLAG() { return INTER_EDITION_FLAG;  }
    public void setINTER_EDITION_FLAG(boolean SEPARATELY_SALE_INDICATOR) { this.INTER_EDITION_FLAG = INTER_EDITION_FLAG;  }

    public String getFIRST_PUB_DATE() {      return FIRST_PUB_DATE;   }
    public void setFIRST_PUB_DATE(String FIRST_PUB_DATE) {
        this.FIRST_PUB_DATE = FIRST_PUB_DATE;
    }

    public String getLAST_PUB_DATE() {  return LAST_PUB_DATE;  }
    public void setLAST_PUB_DATE(String LAST_PUB_DATE) {
        this.LAST_PUB_DATE = LAST_PUB_DATE;
    }

    public String getT_EVENT_DESCRIPTION() { return T_EVENT_DESCRIPTION; }
    public void setT_EVENT_DESCRIPTION(String T_EVENT_DESCRIPTION) {
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

    public String getF_FORMAT_TYPE() {  return F_FORMAT_TYPE;   }
    public void setF_FORMAT_TYPE(String F_FORMAT_TYPE) {    this.F_FORMAT_TYPE = F_FORMAT_TYPE;   }

    public String getF_WWORK(){return F_WWORK;}
    public void setF_WWORK(String F_WWORK) {
        this.F_WWORK = F_WWORK;
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

    public String getB_FROMBATCHID() {  return B_FROMBATCHID;  }
    public void setB_FROMBATCHID(String B_FROMBATCHID) {
        this.B_FROMBATCHID = B_FROMBATCHID;
    }

    public String getB_TOBATCHID() {  return B_TOBATCHID;  }
    public void setB_TOBATCHID(String B_TOBATCHID) {
        this.B_TOBATCHID = B_TOBATCHID;
    }

    public String getMANIF_IDENTIFIER_ID() {  return MANIF_IDENTIFIER_ID;  }
    public void setMANIF_IDENTIFIER_ID(String MANIF_IDENTIFIER_ID) {
        this.MANIF_IDENTIFIER_ID = MANIF_IDENTIFIER_ID;
    }

    private String IDENTIFIER;
    public String getIDENTIFIER() {  return IDENTIFIER;  }
    public void setIDENTIFIER(String IDENTIFIER) {
        this.IDENTIFIER = IDENTIFIER;
    }

    private String S_IDENTIFIER;
    public String getS_IDENTIFIER() {  return S_IDENTIFIER;  }
    public void setS_IDENTIFIER(String S_IDENTIFIER) {
        this.S_IDENTIFIER = S_IDENTIFIER;
    }

    public String getEFFECTIVE_START_DATE() {  return EFFECTIVE_START_DATE;  }
    public void setEFFECTIVE_START_DATE(String EFFECTIVE_START_DATE) {
        this.EFFECTIVE_START_DATE = EFFECTIVE_START_DATE;
    }

    public String getEFFECTIVE_END_DATE() {  return EFFECTIVE_END_DATE;  }
    public void setEFFECTIVE_END_DATE(String EFFECTIVE_END_DATE) {
        this.EFFECTIVE_END_DATE = EFFECTIVE_END_DATE;
    }

    public String getF_MANIFESTATION() { return F_MANIFESTATION;  }
    public void setF_MANIFESTATION(String F_MANIFESTATION) { this.F_MANIFESTATION = F_MANIFESTATION;  }



}
