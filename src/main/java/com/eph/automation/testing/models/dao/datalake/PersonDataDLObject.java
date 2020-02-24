package com.eph.automation.testing.models.dao.datalake;

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
