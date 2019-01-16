package com.eph.automation.testing.models.dao;

/**
 * Created by RAVIVARMANS on 26/11/2018.
 */
public class ProductDataObject {

    //ProductWork Entities
    public String PRIMARY_ISBN;

    public String WORK_TITLE;
    public String WORK_SUBTITLE;

    //ProductManifestation Entites

    public String PRODUCT_ID;
    public String DAC_KEY;
    public String PROJECT_NUM;
    public String ISSN_L;
    public String JOURNAL_NUMBER;
    public String ELECTRONIC_RIGHTS_IND;
    public String BOOK_EDITION_NAME;
    public String BOOK_VOLUME_NAME;
    public String PMC;
    public String PMG;
    public String WORK_ID;
    public String WORK_STATUS;
    public String WORK_SUBSTATUS;
    public String WORK_TYPE;
    public String IMPRINT;
    public String OPEN_ACCESS_JNL_TYPE_CODE;
    public String PRODUCT_WORK_ID;
    public String F_ACC_PROD_HIERARCHY;
    public String F_RESPONSIBILITY_CENTRE;
    public String F_OPCO_R12;
    public String PRODUCT_WORK_PUB_DATE;
    public String JOURNAL_ACRONYM;

    public String getB_CLASSNAME() {
        return B_CLASSNAME;
    }

    public void setB_CLASSNAME(String b_CLASSNAME) {
        B_CLASSNAME = b_CLASSNAME;
    }

    public String B_CLASSNAME;

    public Integer getWorkCountPmx() {
        return workCountPmx;
    }

    public void setWorkCountPmx(Integer workCountPmx) {
        this.workCountPmx = workCountPmx;
    }

    public Integer workCountPmx;

    public Integer getWorkCountPMXSTG() {
        return workCountPMXSTG;
    }

    public void setWorkCountPMXSTG(Integer workCountPMXSTG) {
        this.workCountPMXSTG = workCountPMXSTG;
    }

    public Integer workCountPMXSTG;

    public Integer getWorkCountEPH() {
        return workCountEPH;
    }

    public void setWorkCountEPH(Integer workCountEPH) {
        this.workCountEPH = workCountEPH;
    }

    public Integer workCountEPH;

    public String getRandom_value() {
        return random_value;
    }

    public void setRandom_value(String random_value) {
        this.random_value = random_value;
    }

    public String random_value;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProductDataObject that = (ProductDataObject) o;

        return !(PRIMARY_ISBN != null ? !PRIMARY_ISBN.equals(that.PRIMARY_ISBN) : that.PRIMARY_ISBN != null);

    }

    @Override
    public int hashCode() {
        return PRIMARY_ISBN != null ? PRIMARY_ISBN.hashCode() : 0;
    }

    public void setPRODUCT_ID(String PRODUCT_ID) {
        this.PRODUCT_ID = PRODUCT_ID;
    }

    public void setWORK_TITLE(String WORK_TITLE) {
        this.WORK_TITLE = WORK_TITLE;
    }

    public void setWORK_SUBTITLE(String WORK_SUBTITLE) {
        this.WORK_SUBTITLE = WORK_SUBTITLE;
    }

    public void setDAC_KEY(String DAC_KEY) {
        this.DAC_KEY = DAC_KEY;
    }

    public void setPRIMARY_ISBN(String PRIMARY_ISBN) {
        this.PRIMARY_ISBN = PRIMARY_ISBN;
    }

    public void setPROJECT_NUM(String PROJECT_NUM) {
        this.PROJECT_NUM = PROJECT_NUM;
    }

    public void setISSN_L(String ISSN_L) {
        this.ISSN_L = ISSN_L;
    }

    public void setJOURNAL_NUMBER(String JOURNAL_NUMBER) {
        this.JOURNAL_NUMBER = JOURNAL_NUMBER;
    }

    public void setELECTRONIC_RIGHTS_IND(String ELECTRONIC_RIGHTS_IND) {
        this.ELECTRONIC_RIGHTS_IND = ELECTRONIC_RIGHTS_IND;
    }

    public void setBOOK_EDITION_NAME(String BOOK_EDITION_NAME) {
        this.BOOK_EDITION_NAME = BOOK_EDITION_NAME;
    }

    public void setBOOK_VOLUME_NAME(String BOOK_VOLUME_NAME) {
        this.BOOK_VOLUME_NAME = BOOK_VOLUME_NAME;
    }

    public void setPMC(String PMC) {
        this.PMC = PMC;
    }

    public void setPMG(String PMG) {
        this.PMG = PMG;
    }

    public void setWORK_STATUS(String WORK_STATUS) {
        this.WORK_STATUS = WORK_STATUS;
    }

    public void setWORK_SUBSTATUS(String WORK_SUBSTATUS) {
        this.WORK_SUBSTATUS = WORK_SUBSTATUS;
    }

    public void setWORK_TYPE(String WORK_TYPE) {
        this.WORK_TYPE = WORK_TYPE;
    }

    public void setIMPRINT(String IMPRINT) {
        this.IMPRINT = IMPRINT;
    }

    public void setOPEN_ACCESS_JNL_TYPE_CODE(String OPEN_ACCESS_JNL_TYPE_CODE) {
        this.OPEN_ACCESS_JNL_TYPE_CODE = OPEN_ACCESS_JNL_TYPE_CODE;
    }

    public void setPRODUCT_WORK_ID(String PRODUCT_WORK_ID) {
        this.PRODUCT_WORK_ID = PRODUCT_WORK_ID;
    }

    public void setF_ACC_PROD_HIERARCHY(String f_ACC_PROD_HIERARCHY) {
        F_ACC_PROD_HIERARCHY = f_ACC_PROD_HIERARCHY;
    }

    public void setF_RESPONSIBILITY_CENTRE(String f_RESPONSIBILITY_CENTRE) {
        F_RESPONSIBILITY_CENTRE = f_RESPONSIBILITY_CENTRE;
    }

    public void setF_OPCO_R12(String f_OPCO_R12) {
        F_OPCO_R12 = f_OPCO_R12;
    }

    public void setPRODUCT_WORK_PUB_DATE(String PRODUCT_WORK_PUB_DATE) {
        this.PRODUCT_WORK_PUB_DATE = PRODUCT_WORK_PUB_DATE;
    }

    public void setJOURNAL_ACRONYM(String JOURNAL_ACRONYM) {
        this.JOURNAL_ACRONYM = JOURNAL_ACRONYM;
    }

    public void setWORK_ID(String WORK_ID) {
        this.WORK_ID = WORK_ID;
    }

    @Override
    public String toString() {
        return "ProductDataObject{" +
                "PRIMARY_ISBN='" + PRIMARY_ISBN + '\'' +
                '}';
    }
}
