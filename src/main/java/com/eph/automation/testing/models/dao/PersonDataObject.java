package com.eph.automation.testing.models.dao;

import java.util.Objects;

/**
 * Created by Bistra Drazheva on 13/03/2019
 */
public class PersonDataObject {

    private String PERSON_SOURCE_REF;
    private String PERSON_FIRST_NAME;
    private String PERSON_FAMILY_NAME;
    private String PEOPLEHUB_ID;
    private String DQ_ERR;

    //SA_PERSON
    private String B_LOADID;
    private String B_CLASSNAME;
    private String PERSON_ID;


    public String getPERSON_SOURCE_REF() {
        return PERSON_SOURCE_REF;
    }

    public void setPERSON_SOURCE_REF(String PERSON_SOURCE_REF) {
        this.PERSON_SOURCE_REF = PERSON_SOURCE_REF;
    }

    public String getPERSON_FIRST_NAME() {
        return PERSON_FIRST_NAME;
    }

    public void setPERSON_FIRST_NAME(String PERSON_FIRST_NAME) {
        this.PERSON_FIRST_NAME = PERSON_FIRST_NAME;
    }

    public String getPERSON_FAMILY_NAME() {
        return PERSON_FAMILY_NAME;
    }

    public void setPERSON_FAMILY_NAME(String PERSON_FAMILY_NAME) {
        this.PERSON_FAMILY_NAME = PERSON_FAMILY_NAME;
    }

    public String getB_LOADID() {
        return B_LOADID;
    }

    public void setB_LOADID(String b_LOADID) {
        B_LOADID = b_LOADID;
    }

    public String getB_CLASSNAME() {
        return B_CLASSNAME;
    }

    public void setB_CLASSNAME(String b_CLASSNAME) {
        B_CLASSNAME = b_CLASSNAME;
    }

    public String getPERSON_ID() {
        return PERSON_ID;
    }

    public void setPERSON_ID(String PERSON_ID) {
        this.PERSON_ID = PERSON_ID;
    }

    public String getPEOPLEHUB_ID() {
        return PEOPLEHUB_ID;
    }

    public void setPEOPLEHUB_ID(String PEOPLEHUB_ID) {
        this.PEOPLEHUB_ID = PEOPLEHUB_ID;
    }

    public String getDQ_ERR() {
        return DQ_ERR;
    }

    public void setDQ_ERR(String DQ_ERR) {
        this.DQ_ERR = DQ_ERR;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PersonDataObject that = (PersonDataObject) o;
        return Objects.equals(PERSON_SOURCE_REF, that.PERSON_SOURCE_REF) &&
                Objects.equals(PERSON_FIRST_NAME, that.PERSON_FIRST_NAME) &&
                Objects.equals(PERSON_FAMILY_NAME, that.PERSON_FAMILY_NAME) &&
                Objects.equals(PEOPLEHUB_ID, that.PEOPLEHUB_ID) &&
                Objects.equals(DQ_ERR, that.DQ_ERR) &&
                Objects.equals(B_LOADID, that.B_LOADID) &&
                Objects.equals(B_CLASSNAME, that.B_CLASSNAME) &&
                Objects.equals(PERSON_ID, that.PERSON_ID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(PERSON_SOURCE_REF, PERSON_FIRST_NAME, PERSON_FAMILY_NAME, PEOPLEHUB_ID, DQ_ERR, B_LOADID, B_CLASSNAME, PERSON_ID);
    }

    @Override
    public String toString() {
        return "PersonDataObject{" +
                "PERSON_SOURCE_REF='" + PERSON_SOURCE_REF + '\'' +
                ", PERSON_FIRST_NAME='" + PERSON_FIRST_NAME + '\'' +
                ", PERSON_FAMILY_NAME='" + PERSON_FAMILY_NAME + '\'' +
                ", PEOPLEHUB_ID='" + PEOPLEHUB_ID + '\'' +
                ", DQ_ERR='" + DQ_ERR + '\'' +
                ", B_LOADID='" + B_LOADID + '\'' +
                ", B_CLASSNAME='" + B_CLASSNAME + '\'' +
                ", PERSON_ID='" + PERSON_ID + '\'' +
                '}';
    }
}
