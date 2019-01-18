package com.eph.automation.testing.models.dao;

import java.util.Objects;

public class ManifestationDataObject {
    public String MANIFESTATION_ID;
    public String MANIFESTATION_KEY_TITLE;
    public String ISBN;
    public String ISSN;
    public String COVER_HEIGHT;
    public String COVER_WIDTH;
    public String PAGE_HEIGHT;
    public String PAGE_WIDTH;
    public String WEIGHT;
    public String CARTON_QTY;
    public String INTERNATIONAL_EDITION_IND;
    public String COPYRIGHT_DATE;
    public String F_PRODUCT_MANIFESTATION_TYP;
    public String FORMAT_TXT;
    public String MANIFESTATION_STATUS;
    public String PRODUCT_MANIFESTATION_ID;
    public String WORK_TYPE_ID;
    public String MANIFESTATTION_SUBTYPE;
    public String COMMODITY;
    public String MANIFESTATION_SUBSTATUS;


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

    public String getPRODUCT_MANIFESTATION_ID() {
        return PRODUCT_MANIFESTATION_ID;
    }

    public void setPRODUCT_MANIFESTATION_ID(String PRODUCT_MANIFESTATION_ID) {
        this.PRODUCT_MANIFESTATION_ID = PRODUCT_MANIFESTATION_ID;
    }

    public String getWORK_TYPE_ID() {
        return WORK_TYPE_ID;
    }

    public void setWORK_TYPE_ID(String WORK_TYPE_ID) {
        this.WORK_TYPE_ID = WORK_TYPE_ID;
    }

    public String getMANIFESTATTION_SUBTYPE() {
        return MANIFESTATTION_SUBTYPE;
    }

    public void setMANIFESTATTION_SUBTYPE(String MANIFESTATTION_SUBTYPE) {
        this.MANIFESTATTION_SUBTYPE = MANIFESTATTION_SUBTYPE;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ManifestationDataObject that = (ManifestationDataObject) o;
        return  Objects.equals(MANIFESTATION_ID, that.MANIFESTATION_ID) &&
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
                Objects.equals(PRODUCT_MANIFESTATION_ID, that.PRODUCT_MANIFESTATION_ID) &&
                Objects.equals(WORK_TYPE_ID, that.WORK_TYPE_ID) &&
                Objects.equals(MANIFESTATTION_SUBTYPE, that.MANIFESTATTION_SUBTYPE) &&
                Objects.equals(COMMODITY, that.COMMODITY) &&
                Objects.equals(MANIFESTATION_SUBSTATUS, that.MANIFESTATION_SUBSTATUS);
    }

    @Override
    public int hashCode() {
        return Objects.hash(MANIFESTATION_ID, MANIFESTATION_KEY_TITLE, ISBN, ISSN, COVER_HEIGHT, COVER_WIDTH, PAGE_HEIGHT, PAGE_WIDTH, WEIGHT, CARTON_QTY, INTERNATIONAL_EDITION_IND, COPYRIGHT_DATE, F_PRODUCT_MANIFESTATION_TYP, FORMAT_TXT, MANIFESTATION_STATUS, PRODUCT_MANIFESTATION_ID, WORK_TYPE_ID, MANIFESTATTION_SUBTYPE, COMMODITY, MANIFESTATION_SUBSTATUS);
    }
}
