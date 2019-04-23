package com.eph.automation.testing.models.dao;

import java.util.Objects;

/**
 * Created by Bistra Drazheva on 17/04/2019
 */
public class SubjectAreaDataObject {
    private String PMX_SOURCE_REF;
    private String SUBJECT_AREA_CODE;
    private String SUBJECT_AREA_NAME;
    private String PARENT_SUBJECT_AREA_REF;
    private String SUBJECT_AREA_TYPE;

    //SA
    private String B_LOADID;
    private String B_CLASSNAME;
    private String SUBJECT_AREA_ID;
    private int F_PARENT_SUBJECT_AREA;


    public String getPMX_SOURCE_REF() {
        return PMX_SOURCE_REF;
    }

    public void setPMX_SOURCE_REF(String PMX_SOURCE_REF) {
        this.PMX_SOURCE_REF = PMX_SOURCE_REF;
    }

    public String getSUBJECT_AREA_CODE() {
        return SUBJECT_AREA_CODE;
    }

    public void setSUBJECT_AREA_CODE(String SUBJECT_AREA_CODE) {
        this.SUBJECT_AREA_CODE = SUBJECT_AREA_CODE;
    }

    public String getSUBJECT_AREA_NAME() {
        return SUBJECT_AREA_NAME;
    }

    public void setSUBJECT_AREA_NAME(String SUBJECT_AREA_NAME) {
        this.SUBJECT_AREA_NAME = SUBJECT_AREA_NAME;
    }

    public String getPARENT_SUBJECT_AREA_REF() {
        return PARENT_SUBJECT_AREA_REF;
    }

    public void setPARENT_SUBJECT_AREA_REF(String PARENT_SUBJECT_AREA_REF) {
        this.PARENT_SUBJECT_AREA_REF = PARENT_SUBJECT_AREA_REF;
    }

    public String getSUBJECT_AREA_TYPE() {
        return SUBJECT_AREA_TYPE;
    }

    public void setSUBJECT_AREA_TYPE(String SUBJECT_AREA_TYPE) {
        this.SUBJECT_AREA_TYPE = SUBJECT_AREA_TYPE;
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

    public String getSUBJECT_AREA_ID() {
        return SUBJECT_AREA_ID;
    }

    public void setSUBJECT_AREA_ID(String SUBJECT_AREA_ID) {
        this.SUBJECT_AREA_ID = SUBJECT_AREA_ID;
    }

    public int getF_PARENT_SUBJECT_AREA() {
        return F_PARENT_SUBJECT_AREA;
    }

    public void setF_PARENT_SUBJECT_AREA(int f_PARENT_SUBJECT_AREA) {
        F_PARENT_SUBJECT_AREA = f_PARENT_SUBJECT_AREA;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubjectAreaDataObject that = (SubjectAreaDataObject) o;
        return Objects.equals(PMX_SOURCE_REF, that.PMX_SOURCE_REF) &&
                Objects.equals(SUBJECT_AREA_CODE, that.SUBJECT_AREA_CODE) &&
                Objects.equals(SUBJECT_AREA_NAME, that.SUBJECT_AREA_NAME) &&
                Objects.equals(PARENT_SUBJECT_AREA_REF, that.PARENT_SUBJECT_AREA_REF) &&
                Objects.equals(SUBJECT_AREA_TYPE, that.SUBJECT_AREA_TYPE) &&
                Objects.equals(B_LOADID, that.B_LOADID) &&
                Objects.equals(B_CLASSNAME, that.B_CLASSNAME) &&
                Objects.equals(SUBJECT_AREA_ID, that.SUBJECT_AREA_ID) &&
                Objects.equals(F_PARENT_SUBJECT_AREA, that.F_PARENT_SUBJECT_AREA);
    }

    @Override
    public int hashCode() {
        return Objects.hash(PMX_SOURCE_REF, SUBJECT_AREA_CODE, SUBJECT_AREA_NAME, PARENT_SUBJECT_AREA_REF, SUBJECT_AREA_TYPE, B_LOADID, B_CLASSNAME, SUBJECT_AREA_ID, F_PARENT_SUBJECT_AREA);
    }


}
