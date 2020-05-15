package com.eph.automation.testing.services.db.sql;
/*
*created by Nishant @ 5 May 2020
* */


public class WorkRelationshipDataObject {
    private String F_RELATIONSHIP_TYPE;
    private String EFFECTIVE_START_DATE;
    private String F_CHILD;
    private String F_PARENT;

    public String getF_RELATIONSHIP_TYPE() {return F_RELATIONSHIP_TYPE;}
    public void setF_RELATIONSHIP_TYPE(String f_RELATIONSHIP_TYPE) {F_RELATIONSHIP_TYPE = f_RELATIONSHIP_TYPE;}

    public String getEFFECTIVE_START_DATE() {return EFFECTIVE_START_DATE;}
    public void setEFFECTIVE_START_DATE(String EFFECTIVE_START_DATE) {this.EFFECTIVE_START_DATE = EFFECTIVE_START_DATE;}

    public String getF_PARENT() {return F_PARENT;}
    public void setF_PARENT(String f_PARENT) {F_PARENT = f_PARENT;}

    public String getF_CHILD() {return F_CHILD;}
    public void setF_CHILD(String f_CHILD) {F_CHILD = f_CHILD;}

    @Override
    public String toString() {
        return "ProductRelationshipDataObject{" +
                ", F_RELATIONSHIP_TYPE='" + F_RELATIONSHIP_TYPE + '\'' +
                ", EFFECTIVE_START_DATE='" + EFFECTIVE_START_DATE + '\'' +
                ", F_PARENT='" + F_PARENT + '\'' +
                ", F_CHILD='" + F_CHILD + '\'' +
                '}';
    }

}
