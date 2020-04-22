package com.eph.automation.testing.models.dao.EPHDataLake;

/**
 * Created by RAVIVARMANS on 26/11/2018.
 */
public class PersonDataDLObject {


    private String PERSON_ID;
    private String B_CLASSNAME;
    private String B_BATCHID;
    private String B_CREDATE;
    private String B_UPDDATE;
    private String B_CREATOR;
    private String B_UPDATOR;
    private String EXTERNAL_REFERENCE;
    private String GIVEN_NAME;
    private String S_GIVEN_NAME;
    private String FAMILY_NAME;
    private String S_FAMILY_NAME;
    private String PEOPLEHUB_ID;
    private String EMAIL;
    private String S_EMAIL;
    private String B_FROMBATCHID;
    private String B_TOBATCHID;
    private String PRODUCT_PERSON_ROLE_ID;
    private String EFFECTIVE_START_DATE;
    private String EFFECTIVE_END_DATE;
    private String F_ROLE;
    private String F_PRODUCT;
    private String F_PERSON;
    private String F_EVENT;
    private String F_WWORK;
    private String WORK_PERSON_ROLE_ID;



    public String getWORK_PERSON_ROLE_ID() {  return WORK_PERSON_ROLE_ID;   }
    public void setWORK_PERSON_ROLE_ID(String WORK_PERSON_ROLE_ID) {
        this.WORK_PERSON_ROLE_ID = WORK_PERSON_ROLE_ID;
    }

    public String getF_WWORK() {  return F_WWORK;   }
    public void setF_WWORK(String F_WWORK) {
        this.F_WWORK = F_WWORK;
    }

    public String getEFFECTIVE_START_DATE() {  return EFFECTIVE_START_DATE;   }
    public void setEFFECTIVE_START_DATE(String EFFECTIVE_START_DATE) {
        this.EFFECTIVE_START_DATE = EFFECTIVE_START_DATE;
    }

    public String getF_PERSON() {  return F_PERSON;   }
    public void setF_PERSON(String F_PERSON) {
        this.F_PERSON = F_PERSON;
    }

    public String getF_EVENT() {  return F_EVENT;   }
    public void setF_EVENT(String F_EVENT) {
        this.F_EVENT = F_EVENT;
    }



    public String getF_ROLE() {  return F_ROLE;   }
    public void setF_ROLE(String F_ROLE) {
        this.F_ROLE = F_ROLE;
    }

    public String getF_PRODUCT() {  return F_PRODUCT;   }
    public void setF_PRODUCT(String F_PRODUCT) {
        this.F_PRODUCT = F_PRODUCT;
    }

    public String getEFFECTIVE_END_DATE() {  return EFFECTIVE_END_DATE;   }
    public void setEFFECTIVE_END_DATE(String EFFECTIVE_END_DATE) {
        this.EFFECTIVE_END_DATE = EFFECTIVE_END_DATE;
    }

    public String getPRODUCT_PERSON_ROLE_ID() {  return PRODUCT_PERSON_ROLE_ID;   }
    public void setPRODUCT_PERSON_ROLE_ID(String PRODUCT_PERSON_ROLE_ID) {
        this.PRODUCT_PERSON_ROLE_ID = PRODUCT_PERSON_ROLE_ID;
    }


    public String getPERSON_ID() {  return PERSON_ID;   }
    public void setPERSON_ID(String PERSON_ID) {
        this.PERSON_ID = PERSON_ID;
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

    public String getEXTERNAL_REFERENCE() { return EXTERNAL_REFERENCE; }
    public void setEXTERNAL_REFERENCE(String EXTERNAL_REFERENCE) { this.EXTERNAL_REFERENCE = EXTERNAL_REFERENCE;  }

    public String getGIVEN_NAME() { return GIVEN_NAME;  }
    public void setGIVEN_NAME(String GIVEN_NAME) { this.GIVEN_NAME = GIVEN_NAME;  }

    public String getS_GIVEN_NAME() { return S_GIVEN_NAME;  }
    public void setS_GIVEN_NAME(String S_GIVEN_NAME) { this.S_GIVEN_NAME = S_GIVEN_NAME;  }

    public String getFAMILY_NAME() { return FAMILY_NAME;  }
    public void setFAMILY_NAME(String FAMILY_NAME) { this.FAMILY_NAME = FAMILY_NAME;  }

    public String getS_FAMILY_NAME() { return S_FAMILY_NAME;  }
    public void setS_FAMILY_NAME(String S_FAMILY_NAME) { this.S_FAMILY_NAME = S_FAMILY_NAME;  }

    public String getPEOPLEHUB_ID() { return PEOPLEHUB_ID;  }
    public void setPEOPLEHUB_ID(String PEOPLEHUB_ID) { this.PEOPLEHUB_ID = PEOPLEHUB_ID;  }

    public String getEMAIL() { return EMAIL;  }
    public void setEMAIL(String EMAIL) { this.EMAIL = EMAIL;  }

    public String getS_EMAIL() { return S_EMAIL;  }
    public void setS_EMAIL(String S_EMAIL) { this.S_EMAIL = S_EMAIL;  }

    public String getB_FROMBATCHID() { return B_FROMBATCHID;  }
    public void setB_FROMBATCHID(String B_FROMBATCHID) { this.B_FROMBATCHID = B_FROMBATCHID;  }

    public String getB_TOBATCHID() { return B_TOBATCHID;  }
    public void setB_TOBATCHID(String B_TOBATCHID) { this.B_TOBATCHID = B_TOBATCHID;  }


}
