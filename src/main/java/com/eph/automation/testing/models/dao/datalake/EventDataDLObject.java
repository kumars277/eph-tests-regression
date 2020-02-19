package com.eph.automation.testing.models.dao.datalake;

/**
 * Created by RAVIVARMANS on 26/11/2018.
 */
public class EventDataDLObject {


    private String EVENT_ID;
    private String B_CLASSNAME;
    private String B_BATCHID;
    private String B_CREDATE;
    private String B_UPDDATE;
    private String B_CREATOR;
    private String B_UPDATOR;
    private String DDATE;
    private String TTIMESTAMP;
    private String DESCRIPTION;
    private String THIRD_PARTY;
    private String WORKFLOW_ID;
    private String F_EVENT_TYPE;
    private String F_WORKFLOW_SOURCE;
    private String F_SELF_ONE;
    private String F_SELF_TWO;
    private String F_SELF_THREE;
    private String F_SELF_FOUR;

    public String getEVENT_ID() {
        return EVENT_ID;
    }
    public void setEVENT_ID(String EVENT_ID) {
        this.EVENT_ID = EVENT_ID;
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

    public String getDDATE(){return DDATE;}
    public void setDDATE(String DDATE) { this.DDATE = DDATE;  }

    public String getTTIMESTAMP() { return TTIMESTAMP; }
    public void setTTIMESTAMP(String TTIMESTAMP) { this.TTIMESTAMP = TTIMESTAMP;  }

    public String getDESCRIPTION() { return DESCRIPTION;  }
    public void setDESCRIPTION(String DESCRIPTION) { this.DESCRIPTION = DESCRIPTION;  }

    public String getTHIRD_PARTY() { return THIRD_PARTY;  }
    public void setTHIRD_PARTY(String THIRD_PARTY) { this.THIRD_PARTY = THIRD_PARTY;  }

    public String getWORKFLOW_ID() { return WORKFLOW_ID;  }
    public void setWORKFLOW_ID(String WORKFLOW_ID) { this.WORKFLOW_ID = WORKFLOW_ID;  }

    public String getF_EVENT_TYPE() { return F_EVENT_TYPE;  }
    public void setF_EVENT_TYPE(String F_EVENT_TYPE) { this.F_EVENT_TYPE = F_EVENT_TYPE;  }

    public String getF_WORKFLOW_SOURCE() { return F_WORKFLOW_SOURCE;  }
    public void setF_WORKFLOW_SOURCE(String F_WORKFLOW_SOURCE) { this.F_WORKFLOW_SOURCE = F_WORKFLOW_SOURCE;  }

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

}
