package com.eph.automation.testing.models.dao;

import java.util.Objects;

/**
 * Created by Bistra Drazheva on 13/03/2019
 */
public class PersonWorkRoleDataObject {
    private String WORK_PERSON_ROLE_SOURCE_REF;
    private String PMX_PARTY_SOURCE_REF;
    private String PMX_WORK_SOURCE_REF;
    private String F_ROLE;
    private String START_DATE;
    private String END_DATE;
    private String UPDATED;



    //SA_WORK_PERSON_ROLE
    private String B_LOADID;
    private String F_EVENT;
    private String B_CLASSNAME;
    private String WORK_PERSON_ROLE_ID;
    private String EFFECTIVE_START_DATE;
    private String EFFECTIVE_END_DATE;
    private String F_WWORK;
    private String F_PERSON;


    public String getSTART_DATE() {
        return START_DATE;
    }

    public void setSTART_DATE(String START_DATE) {
        this.START_DATE = START_DATE;
    }

    public String getEND_DATE() {
        return END_DATE;
    }

    public void setEND_DATE(String END_DATE) {
        this.END_DATE = END_DATE;
    }

    public String getUPDATED() {
        return UPDATED;
    }

    public void setUPDATED(String UPDATED) {
        this.UPDATED = UPDATED;
    }

    public String getWORK_PERSON_ROLE_SOURCE_REF() {
        return WORK_PERSON_ROLE_SOURCE_REF;
    }

    public void setWORK_PERSON_ROLE_SOURCE_REF(String WORK_PERSON_ROLE_SOURCE_REF) {
        this.WORK_PERSON_ROLE_SOURCE_REF = WORK_PERSON_ROLE_SOURCE_REF;
    }

    public String getPMX_PARTY_SOURCE_REF() {
        return PMX_PARTY_SOURCE_REF;
    }

    public void setPMX_PARTY_SOURCE_REF(String PMX_PARTY_SOURCE_REF) {
        this.PMX_PARTY_SOURCE_REF = PMX_PARTY_SOURCE_REF;
    }

    public String getPMX_WORK_SOURCE_REF() {
        return PMX_WORK_SOURCE_REF;
    }

    public void setPMX_WORK_SOURCE_REF(String PMX_WORK_SOURCE_REF) {
        this.PMX_WORK_SOURCE_REF = PMX_WORK_SOURCE_REF;
    }

    public String getF_ROLE() {
        return F_ROLE;
    }

    public void setF_ROLE(String f_ROLE) {
        F_ROLE = f_ROLE;
    }

    public String getB_LOADID() {
        return B_LOADID;
    }

    public void setB_LOADID(String b_LOADID) {
        B_LOADID = b_LOADID;
    }

    public String getF_EVENT() {
        return F_EVENT;
    }

    public void setF_EVENT(String f_EVENT) {
        F_EVENT = f_EVENT;
    }

    public String getB_CLASSNAME() {
        return B_CLASSNAME;
    }

    public void setB_CLASSNAME(String b_CLASSNAME) {
        B_CLASSNAME = b_CLASSNAME;
    }

    public String getWORK_PERSON_ROLE_ID() {
        return WORK_PERSON_ROLE_ID;
    }

    public void setWORK_PERSON_ROLE_ID(String WORK_PERSON_ROLE_ID) {
        this.WORK_PERSON_ROLE_ID = WORK_PERSON_ROLE_ID;
    }

    public String getEFFECTIVE_START_DATE() {
        return EFFECTIVE_START_DATE;
    }

    public void setEFFECTIVE_START_DATE(String EFFECTIVE_START_DATE) {
        this.EFFECTIVE_START_DATE = EFFECTIVE_START_DATE;
    }

    public String getEFFECTIVE_END_DATE() {
        return EFFECTIVE_END_DATE;
    }

    public void setEFFECTIVE_END_DATE(String EFFECTIVE_END_DATE) {
        this.EFFECTIVE_END_DATE = EFFECTIVE_END_DATE;
    }

    public String getF_WWORK() {
        return F_WWORK;
    }

    public void setF_WWORK(String f_WWORK) {
        F_WWORK = f_WWORK;
    }

    public String getF_PERSON() {
        return F_PERSON;
    }

    public void setF_PERSON(String f_PERSON) {
        F_PERSON = f_PERSON;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PersonWorkRoleDataObject that = (PersonWorkRoleDataObject) o;
        return Objects.equals(WORK_PERSON_ROLE_SOURCE_REF, that.WORK_PERSON_ROLE_SOURCE_REF) &&
                Objects.equals(PMX_PARTY_SOURCE_REF, that.PMX_PARTY_SOURCE_REF) &&
                Objects.equals(PMX_WORK_SOURCE_REF, that.PMX_WORK_SOURCE_REF) &&
                Objects.equals(F_ROLE, that.F_ROLE) &&
                Objects.equals(B_LOADID, that.B_LOADID) &&
                Objects.equals(F_EVENT, that.F_EVENT) &&
                Objects.equals(B_CLASSNAME, that.B_CLASSNAME) &&
                Objects.equals(WORK_PERSON_ROLE_ID, that.WORK_PERSON_ROLE_ID) &&
                Objects.equals(EFFECTIVE_START_DATE, that.EFFECTIVE_START_DATE) &&
                Objects.equals(EFFECTIVE_END_DATE, that.EFFECTIVE_END_DATE) &&
                Objects.equals(F_WWORK, that.F_WWORK) &&
                Objects.equals(F_PERSON, that.F_PERSON);
    }

    @Override
    public String toString() {
        return "PersonWorkRoleDataObject{" +
                "WORK_PERSON_ROLE_SOURCE_REF='" + WORK_PERSON_ROLE_SOURCE_REF + '\'' +
                ", PMX_PARTY_SOURCE_REF='" + PMX_PARTY_SOURCE_REF + '\'' +
                ", PMX_WORK_SOURCE_REF='" + PMX_WORK_SOURCE_REF + '\'' +
                ", F_ROLE='" + F_ROLE + '\'' +
                ", B_LOADID='" + B_LOADID + '\'' +
                ", F_EVENT='" + F_EVENT + '\'' +
                ", B_CLASSNAME='" + B_CLASSNAME + '\'' +
                ", WORK_PERSON_ROLE_ID='" + WORK_PERSON_ROLE_ID + '\'' +
                ", EFFECTIVE_START_DATE='" + EFFECTIVE_START_DATE + '\'' +
                ", EFFECTIVE_END_DATE='" + EFFECTIVE_END_DATE + '\'' +
                ", F_WWORK='" + F_WWORK + '\'' +
                ", F_PERSON='" + F_PERSON + '\'' +
                '}';
    }
}
