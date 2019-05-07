package com.eph.automation.testing.models.dao;

import java.util.Objects;

public class  ManifestationDataObject {
    private String MANIFESTATION_ID;
    private String MANIFESTATION_KEY_TITLE;
    private String ISBN;
    private String ISSN;
    private String COVER_HEIGHT;
    private String COVER_WIDTH;
    private String PAGE_HEIGHT;
    private String PAGE_WIDTH;
    private String WEIGHT;
    private String CARTON_QTY;
    private String INTERNATIONAL_EDITION_IND;
    private String COPYRIGHT_DATE;
    private String F_PRODUCT_MANIFESTATION_TYP;
    private String FORMAT_TXT;
    private String MANIFESTATION_STATUS;
    private int PRODUCT_MANIFESTATION_ID;
    private String F_PRODUCT_WORK;
    private String WORK_TYPE_ID;
    private String MANIFESTATION_SUBTYPE;
    private String COMMODITY;
    private String MANIFESTATION_SUBSTATUS;
    private String DQ_ERR;

    //SA_MANIFESTATION columns
    private String F_EVENT;
    private String B_CLASSNAME;
    private int PMX_SOURCE_REFERENCE;
    private String INTER_EDITION_FLAG;
    private String FIRST_PUB_DATE;
    private String LAST_PUB_DATE;
    private String F_TYPE;
    private String F_STATUS;
    private String F_FORMAT_TYPE;
    private String F_WWORK;


    public String getMANIFESTATION_ID() {
        return MANIFESTATION_ID;
    }

    public void setMANIFESTATION_ID(String MANIFESTATION_ID) {
        this.MANIFESTATION_ID = MANIFESTATION_ID;
    }

    public String getMANIFESTATION_KEY_TITLE() {
        return MANIFESTATION_KEY_TITLE;
    }

    public void setMANIFESTATION_KEY_TITLE(String MANIFESTATION_KEY_TITLE) {
        this.MANIFESTATION_KEY_TITLE = MANIFESTATION_KEY_TITLE;
    }

    public String getISBN() {
        return ISBN;
    }

    public void setISBN(String ISBN) {
        this.ISBN = ISBN;
    }

    public String getISSN() {
        return ISSN;
    }

    public void setISSN(String ISSN) {
        this.ISSN = ISSN;
    }

    public String getCOVER_HEIGHT() {
        return COVER_HEIGHT;
    }

    public void setCOVER_HEIGHT(String COVER_HEIGHT) {
        this.COVER_HEIGHT = COVER_HEIGHT;
    }

    public String getCOVER_WIDTH() {
        return COVER_WIDTH;
    }

    public void setCOVER_WIDTH(String COVER_WIDTH) {
        this.COVER_WIDTH = COVER_WIDTH;
    }

    public String getPAGE_HEIGHT() {
        return PAGE_HEIGHT;
    }

    public void setPAGE_HEIGHT(String PAGE_HEIGHT) {
        this.PAGE_HEIGHT = PAGE_HEIGHT;
    }

    public String getPAGE_WIDTH() {
        return PAGE_WIDTH;
    }

    public void setPAGE_WIDTH(String PAGE_WIDTH) {
        this.PAGE_WIDTH = PAGE_WIDTH;
    }

    public String getWEIGHT() {
        return WEIGHT;
    }

    public void setWEIGHT(String WEIGHT) {
        this.WEIGHT = WEIGHT;
    }

    public String getCARTON_QTY() {
        return CARTON_QTY;
    }

    public void setCARTON_QTY(String CARTON_QTY) {
        this.CARTON_QTY = CARTON_QTY;
    }

    public String getINTERNATIONAL_EDITION_IND() {
        return INTERNATIONAL_EDITION_IND;
    }

    public void setINTERNATIONAL_EDITION_IND(String INTERNATIONAL_EDITION_IND) {
        this.INTERNATIONAL_EDITION_IND = INTERNATIONAL_EDITION_IND;
    }

    public String getCOPYRIGHT_DATE() {
        return COPYRIGHT_DATE;
    }

    public void setCOPYRIGHT_DATE(String COPYRIGHT_DATE) {
        this.COPYRIGHT_DATE = COPYRIGHT_DATE;
    }

    public String getF_PRODUCT_MANIFESTATION_TYP() {
        return F_PRODUCT_MANIFESTATION_TYP;
    }

    public void setF_PRODUCT_MANIFESTATION_TYP(String f_PRODUCT_MANIFESTATION_TYP) {
        F_PRODUCT_MANIFESTATION_TYP = f_PRODUCT_MANIFESTATION_TYP;
    }

    public String getF_PRODUCT_WORK() {
        return F_PRODUCT_WORK;
    }

    public void setF_PRODUCT_WORK(String f_PRODUCT_WORK) {
        F_PRODUCT_WORK = f_PRODUCT_WORK;
    }

    public String getFORMAT_TXT() {
        return FORMAT_TXT;
    }

    public void setFORMAT_TXT(String FORMAT_TXT) {
        this.FORMAT_TXT = FORMAT_TXT;
    }

    public String getMANIFESTATION_STATUS() {
        return MANIFESTATION_STATUS;
    }

    public void setMANIFESTATION_STATUS(String MANIFESTATION_STATUS) {
        this.MANIFESTATION_STATUS = MANIFESTATION_STATUS;
    }

    public int getPRODUCT_MANIFESTATION_ID() {
        return PRODUCT_MANIFESTATION_ID;
    }

    public void setPRODUCT_MANIFESTATION_ID(int PRODUCT_MANIFESTATION_ID) {
        this.PRODUCT_MANIFESTATION_ID = PRODUCT_MANIFESTATION_ID;
    }

    public String getWORK_TYPE_ID() {
        return WORK_TYPE_ID;
    }

    public void setWORK_TYPE_ID(String WORK_TYPE_ID) {
        this.WORK_TYPE_ID = WORK_TYPE_ID;
    }

    public String getMANIFESTATION_SUBTYPE() {
        return MANIFESTATION_SUBTYPE;
    }

    public void setMANIFESTATION_SUBTYPE(String MANIFESTATION_SUBTYPE) {
        this.MANIFESTATION_SUBTYPE = MANIFESTATION_SUBTYPE;
    }

    public String getCOMMODITY() {
        return COMMODITY;
    }

    public void setCOMMODITY(String COMMODITY) {
        this.COMMODITY = COMMODITY;
    }

    public String getMANIFESTATION_SUBSTATUS() {
        return MANIFESTATION_SUBSTATUS;
    }

    public void setMANIFESTATION_SUBSTATUS(String MANIFESTATION_SUBSTATUS) {
        this.MANIFESTATION_SUBSTATUS = MANIFESTATION_SUBSTATUS;
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
        ManifestationDataObject that = (ManifestationDataObject) o;
        return PRODUCT_MANIFESTATION_ID == that.PRODUCT_MANIFESTATION_ID &&
                PMX_SOURCE_REFERENCE == that.PMX_SOURCE_REFERENCE &&
                Objects.equals(MANIFESTATION_ID, that.MANIFESTATION_ID) &&
                Objects.equals(MANIFESTATION_KEY_TITLE, that.MANIFESTATION_KEY_TITLE) &&
                Objects.equals(ISBN, that.ISBN) &&
                Objects.equals(ISSN, that.ISSN) &&
                Objects.equals(COVER_HEIGHT, that.COVER_HEIGHT) &&
                Objects.equals(COVER_WIDTH, that.COVER_WIDTH) &&
                Objects.equals(PAGE_HEIGHT, that.PAGE_HEIGHT) &&
                Objects.equals(PAGE_WIDTH, that.PAGE_WIDTH) &&
                Objects.equals(WEIGHT, that.WEIGHT) &&
                Objects.equals(CARTON_QTY, that.CARTON_QTY) &&
                Objects.equals(INTERNATIONAL_EDITION_IND, that.INTERNATIONAL_EDITION_IND) &&
                Objects.equals(COPYRIGHT_DATE, that.COPYRIGHT_DATE) &&
                Objects.equals(F_PRODUCT_MANIFESTATION_TYP, that.F_PRODUCT_MANIFESTATION_TYP) &&
                Objects.equals(FORMAT_TXT, that.FORMAT_TXT) &&
                Objects.equals(MANIFESTATION_STATUS, that.MANIFESTATION_STATUS) &&
                Objects.equals(F_PRODUCT_WORK, that.F_PRODUCT_WORK) &&
                Objects.equals(WORK_TYPE_ID, that.WORK_TYPE_ID) &&
                Objects.equals(MANIFESTATION_SUBTYPE, that.MANIFESTATION_SUBTYPE) &&
                Objects.equals(COMMODITY, that.COMMODITY) &&
                Objects.equals(MANIFESTATION_SUBSTATUS, that.MANIFESTATION_SUBSTATUS) &&
                Objects.equals(DQ_ERR, that.DQ_ERR) &&
                Objects.equals(F_EVENT, that.F_EVENT) &&
                Objects.equals(B_CLASSNAME, that.B_CLASSNAME) &&
                Objects.equals(INTER_EDITION_FLAG, that.INTER_EDITION_FLAG) &&
                Objects.equals(FIRST_PUB_DATE, that.FIRST_PUB_DATE) &&
                Objects.equals(LAST_PUB_DATE, that.LAST_PUB_DATE) &&
                Objects.equals(F_TYPE, that.F_TYPE) &&
                Objects.equals(F_STATUS, that.F_STATUS) &&
                Objects.equals(F_FORMAT_TYPE, that.F_FORMAT_TYPE) &&
                Objects.equals(F_WWORK, that.F_WWORK);
    }

    @Override
    public int hashCode() {
        return Objects.hash(MANIFESTATION_ID, MANIFESTATION_KEY_TITLE, ISBN, ISSN, COVER_HEIGHT, COVER_WIDTH, PAGE_HEIGHT, PAGE_WIDTH, WEIGHT, CARTON_QTY, INTERNATIONAL_EDITION_IND, COPYRIGHT_DATE, F_PRODUCT_MANIFESTATION_TYP, FORMAT_TXT, MANIFESTATION_STATUS, PRODUCT_MANIFESTATION_ID, WORK_TYPE_ID, MANIFESTATION_SUBTYPE, COMMODITY, MANIFESTATION_SUBSTATUS);
    }


    //SA_MANIFESTATION columns


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

    public int getPMX_SOURCE_REFERENCE() {
        return PMX_SOURCE_REFERENCE;
    }

    public void setPMX_SOURCE_REFERENCE(int PMX_SOURCE_REFERENCE) {
        this.PMX_SOURCE_REFERENCE = PMX_SOURCE_REFERENCE;
    }

    public String getINTER_EDITION_FLAG() {
        return INTER_EDITION_FLAG;
    }

    public void setINTER_EDITION_FLAG(String INTER_EDITION_FLAG) {
        this.INTER_EDITION_FLAG = INTER_EDITION_FLAG;
    }

    public String getFIRST_PUB_DATE() {
        return FIRST_PUB_DATE;
    }

    public void setFIRST_PUB_DATE(String FIRST_PUB_DATE) {
        this.FIRST_PUB_DATE = FIRST_PUB_DATE;
    }

    public String getLAST_PUB_DATE() {
        return LAST_PUB_DATE;
    }

    public void setLAST_PUB_DATE(String LAST_PUB_DATE) {
        this.LAST_PUB_DATE = LAST_PUB_DATE;
    }

    public String getF_TYPE() {
        return F_TYPE;
    }

    public void setF_TYPE(String f_TYPE) {
        F_TYPE = f_TYPE;
    }

    public String getF_STATUS() {
        return F_STATUS;
    }

    public void setF_STATUS(String f_STATUS) {
        F_STATUS = f_STATUS;
    }

    public String getF_FORMAT_TYPE() {
        return F_FORMAT_TYPE;
    }

    public void setF_FORMAT_TYPE(String f_FORMAT_TYPE) {
        F_FORMAT_TYPE = f_FORMAT_TYPE;
    }

    public String getF_WWORK() {
        return F_WWORK;
    }

    public void setF_WWORK(String f_WWORK) {
        F_WWORK = f_WWORK;
    }
}
