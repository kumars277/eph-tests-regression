package com.eph.automation.testing.models.dao;

import java.util.Objects;

/**
 * Created by Bistra Drazheva on 03/04/2019
 * updated by Nishant @ 22 Apr 2020
 */
public class AccountableProductDataObject {
    private int PRODUCT_WORK_ID;
    private String ACC_PROD_ID;
    private String ACC_PROD_NAME;
    private String PARENT_ACC_PROD;
    private String PRODUCT_GROUP_TYPE_NAME;
    private String ACC_PROD_HIERARHY;
    private String AC_ELS_PROD_ID;
    private String PRODUCT_WORK_TITLE;
    private String WORK_ELS_PROD_ID;
    private String UPDATED;
    private String DQ_ERR;
    private String PMX_SOURCE_REFERENCE;
    private String EXTERNAL_REFERENCE;

    //SA
    private String B_LOADID;
    private String F_EVENT;
    private String B_CLASSNAME;
    private int ACCOUNTABLE_PRODUCT_ID;
    private String GL_PRODUCT_SEGMENT_CODE;
    private String GL_PRODUCT_SEGMENT_NAME;
    private String F_GL_PRODUCT_SEGMENT_PARENT;

    public String getEXTERNAL_REFERENCE() {return EXTERNAL_REFERENCE;}
    public void setEXTERNAL_REFERENCE(String EXTERNAL_REFERENCE) {this.EXTERNAL_REFERENCE = EXTERNAL_REFERENCE;}

    public String getPMX_SOURCE_REFERENCE() {return PMX_SOURCE_REFERENCE;}
    public void setPMX_SOURCE_REFERENCE(String PMX_SOURCE_REFERENCE) {this.PMX_SOURCE_REFERENCE = PMX_SOURCE_REFERENCE;}

    public String getDQ_ERR() {return DQ_ERR;}
    public void setDQ_ERR(String DQ_ERR) {this.DQ_ERR = DQ_ERR;}

    public String getUPDATED() {return UPDATED;}
    public void setUPDATED(String UPDATED) {this.UPDATED = UPDATED;}

    public String getWORK_ELS_PROD_ID() {return WORK_ELS_PROD_ID;}
    public void setWORK_ELS_PROD_ID(String WORK_ELS_PROD_ID) {this.WORK_ELS_PROD_ID = WORK_ELS_PROD_ID;}

    public String getAC_ELS_PROD_ID() {return AC_ELS_PROD_ID;}
    public void setAC_ELS_PROD_ID(String AC_ELS_PROD_ID) {this.AC_ELS_PROD_ID = AC_ELS_PROD_ID;}

    public int getPRODUCT_WORK_ID() {return PRODUCT_WORK_ID;}
    public void setPRODUCT_WORK_ID(int PRODUCT_WORK_ID) {this.PRODUCT_WORK_ID = PRODUCT_WORK_ID;}

    public String getACC_PROD_ID() {return ACC_PROD_ID;}
    public void setACC_PROD_ID(String ACC_PROD_ID) {this.ACC_PROD_ID = ACC_PROD_ID;}

    public String getACC_PROD_NAME() {return ACC_PROD_NAME;}
    public void setACC_PROD_NAME(String ACC_PROD_NAME) {this.ACC_PROD_NAME = ACC_PROD_NAME;}

    public String getPARENT_ACC_PROD() {return PARENT_ACC_PROD;}
    public void setPARENT_ACC_PROD(String PARENT_ACC_PROD) {this.PARENT_ACC_PROD = PARENT_ACC_PROD;}

    public String getPRODUCT_GROUP_TYPE_NAME() {return PRODUCT_GROUP_TYPE_NAME;}
    public void setPRODUCT_GROUP_TYPE_NAME(String PRODUCT_GROUP_TYPE_NAME) {this.PRODUCT_GROUP_TYPE_NAME = PRODUCT_GROUP_TYPE_NAME;}

    public String getB_LOADID() {return B_LOADID;}
    public void setB_LOADID(String b_LOADID) {B_LOADID = b_LOADID;}

    public String getF_EVENT() {return F_EVENT;}
    public void setF_EVENT(String f_EVENT) {F_EVENT = f_EVENT;}

    public String getB_CLASSNAME() {return B_CLASSNAME;}
    public void setB_CLASSNAME(String b_CLASSNAME) {B_CLASSNAME = b_CLASSNAME;}

    public int getACCOUNTABLE_PRODUCT_ID() {return ACCOUNTABLE_PRODUCT_ID;}
    public void setACCOUNTABLE_PRODUCT_ID(int ACCOUNTABLE_PRODUCT_ID) {this.ACCOUNTABLE_PRODUCT_ID = ACCOUNTABLE_PRODUCT_ID;}

    public String getGL_PRODUCT_SEGMENT_CODE() {return GL_PRODUCT_SEGMENT_CODE;}
    public void setGL_PRODUCT_SEGMENT_CODE(String GL_PRODUCT_SEGMENT_CODE) {this.GL_PRODUCT_SEGMENT_CODE = GL_PRODUCT_SEGMENT_CODE;}

    public String getGL_PRODUCT_SEGMENT_NAME() {return GL_PRODUCT_SEGMENT_NAME;}
    public void setGL_PRODUCT_SEGMENT_NAME(String GL_PRODUCT_SEGMENT_NAME) {this.GL_PRODUCT_SEGMENT_NAME = GL_PRODUCT_SEGMENT_NAME;}

    public String getF_GL_PRODUCT_SEGMENT_PARENT() {return F_GL_PRODUCT_SEGMENT_PARENT;}
    public void setF_GL_PRODUCT_SEGMENT_PARENT(String f_GL_PRODUCT_SEGMENT_PARENT) {F_GL_PRODUCT_SEGMENT_PARENT = f_GL_PRODUCT_SEGMENT_PARENT;}

    public String getACC_PROD_HIERARHY() {return ACC_PROD_HIERARHY;}
    public void setACC_PROD_HIERARHY(String ACC_PROD_HIERARHY) {this.ACC_PROD_HIERARHY = ACC_PROD_HIERARHY;}

    public String getPRODUCT_WORK_TITLE() {return PRODUCT_WORK_TITLE;}
    public void setPRODUCT_WORK_TITLE(String PRODUCT_WORK_TITLE) {this.PRODUCT_WORK_TITLE = PRODUCT_WORK_TITLE;}

    @Override
    public String toString() {
        return "AccountableProductDataObject{" +
                "PRODUCT_WORK_ID='" + PRODUCT_WORK_ID + '\'' +
                ", ACC_PROD_ID='" + ACC_PROD_ID + '\'' +
                ", ACC_PROD_NAME='" + ACC_PROD_NAME + '\'' +
                ", PARENT_ACC_PROD='" + PARENT_ACC_PROD + '\'' +
                ", PRODUCT_GROUP_TYPE_NAME='" + PRODUCT_GROUP_TYPE_NAME + '\'' +
                ", B_LOADID='" + B_LOADID + '\'' +
                ", F_EVENT='" + F_EVENT + '\'' +
                ", B_CLASSNAME='" + B_CLASSNAME + '\'' +
                ", ACCOUNTABLE_PRODUCT_ID='" + ACCOUNTABLE_PRODUCT_ID + '\'' +
                ", GL_PRODUCT_SEGMENT_CODE='" + GL_PRODUCT_SEGMENT_CODE + '\'' +
                ", GL_PRODUCT_SEGMENT_NAME='" + GL_PRODUCT_SEGMENT_NAME + '\'' +
                ", F_GL_PRODUCT_SEGMENT_PARENT='" + F_GL_PRODUCT_SEGMENT_PARENT + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccountableProductDataObject that = (AccountableProductDataObject) o;
        return Objects.equals(PRODUCT_WORK_ID, that.PRODUCT_WORK_ID) &&
                Objects.equals(ACC_PROD_ID, that.ACC_PROD_ID) &&
                Objects.equals(ACC_PROD_NAME, that.ACC_PROD_NAME) &&
                Objects.equals(PARENT_ACC_PROD, that.PARENT_ACC_PROD) &&
                Objects.equals(PRODUCT_GROUP_TYPE_NAME, that.PRODUCT_GROUP_TYPE_NAME) &&
                Objects.equals(B_LOADID, that.B_LOADID) &&
                Objects.equals(F_EVENT, that.F_EVENT) &&
                Objects.equals(B_CLASSNAME, that.B_CLASSNAME) &&
                Objects.equals(ACCOUNTABLE_PRODUCT_ID, that.ACCOUNTABLE_PRODUCT_ID) &&
                Objects.equals(GL_PRODUCT_SEGMENT_CODE, that.GL_PRODUCT_SEGMENT_CODE) &&
                Objects.equals(GL_PRODUCT_SEGMENT_NAME, that.GL_PRODUCT_SEGMENT_NAME) &&
                Objects.equals(F_GL_PRODUCT_SEGMENT_PARENT, that.F_GL_PRODUCT_SEGMENT_PARENT);
    }

    @Override
    public int hashCode() {
        return Objects.hash(PRODUCT_WORK_ID, ACC_PROD_ID, ACC_PROD_NAME, PARENT_ACC_PROD, PRODUCT_GROUP_TYPE_NAME, B_LOADID, F_EVENT, B_CLASSNAME, ACCOUNTABLE_PRODUCT_ID, GL_PRODUCT_SEGMENT_CODE, GL_PRODUCT_SEGMENT_NAME, F_GL_PRODUCT_SEGMENT_PARENT);
    }


}
