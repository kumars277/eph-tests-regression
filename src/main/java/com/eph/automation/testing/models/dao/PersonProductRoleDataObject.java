package com.eph.automation.testing.models.dao;

import java.util.Objects;

/**
 * Created by Bistra Drazheva on 13/03/2019
 */
public class PersonProductRoleDataObject {
    private String PROD_PER_ROLE_SOURCE_REF;
    private String PRODUCT_SOURCE_REF;
    private String PERSON_SOURCE_REF;
    private String F_ROLE;
    private String WORK_ROLE;
    private String START_DATE;
    private String END_DATE;
    private String UPDATED;
    private String EXTERNAL_REFERENCE;

    //SA_PRODUCT_PERSON_ROLE
    private String B_LOADID;
    private String F_EVENT;
    private String B_CLASSNAME;
    private int PRODUCT_PERSON_ROLE_ID;
    private String EFFECTIVE_START_DATE;
    private String EFFECTIVE_END_DATE;
    private String F_PRODUCT;
    private String F_PERSON;


    public String getEXTERNAL_REFERENCE() {
        return EXTERNAL_REFERENCE;
    }

    public void setEXTERNAL_REFERENCE(String EXTERNAL_REFERENCE) {
        this.EXTERNAL_REFERENCE = EXTERNAL_REFERENCE;
    }

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

    public String getPROD_PER_ROLE_SOURCE_REF() {
        return PROD_PER_ROLE_SOURCE_REF;
    }

    public void setPROD_PER_ROLE_SOURCE_REF(String PROD_PER_ROLE_SOURCE_REF) {
        this.PROD_PER_ROLE_SOURCE_REF = PROD_PER_ROLE_SOURCE_REF;
    }

    public String getPRODUCT_SOURCE_REF() {
        return PRODUCT_SOURCE_REF;
    }

    public void setPRODUCT_SOURCE_REF(String PRODUCT_SOURCE_REF) {
        this.PRODUCT_SOURCE_REF = PRODUCT_SOURCE_REF;
    }

    public String getPERSON_SOURCE_REF() {
        return PERSON_SOURCE_REF;
    }

    public void setPERSON_SOURCE_REF(String PERSON_SOURCE_REF) {
        this.PERSON_SOURCE_REF = PERSON_SOURCE_REF;
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

    public int getPRODUCT_PERSON_ROLE_ID() {
        return PRODUCT_PERSON_ROLE_ID;
    }

    public void setPRODUCT_PERSON_ROLE_ID(int PRODUCT_PERSON_ROLE_ID) {
        this.PRODUCT_PERSON_ROLE_ID = PRODUCT_PERSON_ROLE_ID;
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

    public String getF_PRODUCT() {
        return F_PRODUCT;
    }

    public void setF_PRODUCT(String f_PRODUCT) {
        F_PRODUCT = f_PRODUCT;
    }

    public String getF_PERSON() {
        return F_PERSON;
    }

    public void setF_PERSON(String f_PERSON) {
        F_PERSON = f_PERSON;
    }

    public String getWORK_ROLE() {
        return WORK_ROLE;
    }

    public void setWORK_ROLE(String WORK_ROLE) {
        this.WORK_ROLE = WORK_ROLE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PersonProductRoleDataObject that = (PersonProductRoleDataObject) o;
        return Objects.equals(PROD_PER_ROLE_SOURCE_REF, that.PROD_PER_ROLE_SOURCE_REF) &&
                Objects.equals(PRODUCT_SOURCE_REF, that.PRODUCT_SOURCE_REF) &&
                Objects.equals(PERSON_SOURCE_REF, that.PERSON_SOURCE_REF) &&
                Objects.equals(F_ROLE, that.F_ROLE) &&
                Objects.equals(WORK_ROLE, that.WORK_ROLE) &&
                Objects.equals(B_LOADID, that.B_LOADID) &&
                Objects.equals(F_EVENT, that.F_EVENT) &&
                Objects.equals(B_CLASSNAME, that.B_CLASSNAME) &&
                Objects.equals(PRODUCT_PERSON_ROLE_ID, that.PRODUCT_PERSON_ROLE_ID) &&
                Objects.equals(EFFECTIVE_START_DATE, that.EFFECTIVE_START_DATE) &&
                Objects.equals(EFFECTIVE_END_DATE, that.EFFECTIVE_END_DATE) &&
                Objects.equals(F_PRODUCT, that.F_PRODUCT) &&
                Objects.equals(F_PERSON, that.F_PERSON);
    }

    @Override
    public int hashCode() {
        return Objects.hash(PROD_PER_ROLE_SOURCE_REF, PRODUCT_SOURCE_REF, PERSON_SOURCE_REF, F_ROLE, WORK_ROLE, B_LOADID, F_EVENT, B_CLASSNAME, PRODUCT_PERSON_ROLE_ID, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, F_PRODUCT, F_PERSON);
    }

    @Override
    public String toString() {
        return "PersonProductRoleDataObject{" +
                "PROD_PER_ROLE_SOURCE_REF='" + PROD_PER_ROLE_SOURCE_REF + '\'' +
                ", PRODUCT_SOURCE_REF='" + PRODUCT_SOURCE_REF + '\'' +
                ", PERSON_SOURCE_REF='" + PERSON_SOURCE_REF + '\'' +
                ", F_ROLE='" + F_ROLE + '\'' +
                ", WORK_ROLE='" + WORK_ROLE + '\'' +
                ", B_LOADID='" + B_LOADID + '\'' +
                ", F_EVENT='" + F_EVENT + '\'' +
                ", B_CLASSNAME='" + B_CLASSNAME + '\'' +
                ", PRODUCT_PERSON_ROLE_ID='" + PRODUCT_PERSON_ROLE_ID + '\'' +
                ", EFFECTIVE_START_DATE='" + EFFECTIVE_START_DATE + '\'' +
                ", EFFECTIVE_END_DATE='" + EFFECTIVE_END_DATE + '\'' +
                ", F_PRODUCT='" + F_PRODUCT + '\'' +
                ", F_PERSON='" + F_PERSON + '\'' +
                '}';
    }
}
