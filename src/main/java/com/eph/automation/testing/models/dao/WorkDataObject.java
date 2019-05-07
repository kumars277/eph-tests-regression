package com.eph.automation.testing.models.dao;

import java.util.Objects;

/**
 * Created by RAVIVARMANS on 26/11/2018.
 */
public class WorkDataObject {

    //ProductWork Entities
    public String PRIMARY_ISBN;

    public String WORK_TITLE;
    public String WORK_SUBTITLE;

    //ProductManifestation Entites

    public String PRODUCT_ID;

    public String getIDENTIFIER() {
        return IDENTIFIER;
    }

    public void setIDENTIFIER(String IDENTIFIER) {
        this.IDENTIFIER = IDENTIFIER;
    }

    public String IDENTIFIER;


    public String DAC_KEY;
    public String PROJECT_NUM;

    public String getPRIMARY_ISBN() {
        return PRIMARY_ISBN;
    }

    public String getWORK_TITLE() {
        return WORK_TITLE;
    }

    public String getWORK_SUBTITLE() {
        return WORK_SUBTITLE;
    }

    public String getPRODUCT_ID() {
        return PRODUCT_ID;
    }

    public String getDAC_KEY() {
        return DAC_KEY;
    }

    public String getPROJECT_NUM() {
        return PROJECT_NUM;
    }

    public String getISSN_L() {
        return ISSN_L;
    }

    public String getJOURNAL_NUMBER() {
        return JOURNAL_NUMBER;
    }

    public String getELECTRONIC_RIGHTS_IND() {
        return ELECTRONIC_RIGHTS_IND;
    }

    public String getBOOK_EDITION_NAME() {
        return BOOK_EDITION_NAME;
    }

    public String getBOOK_VOLUME_NAME() {
        return BOOK_VOLUME_NAME;
    }

    public String getPMC() {
        return PMC;
    }

    public String getPMG() {
        return PMG;
    }

    public String getWORK_ID() {
        return WORK_ID;
    }

    public String getWORK_STATUS() {
        return WORK_STATUS;
    }

    public String getWORK_SUBSTATUS() {
        return WORK_SUBSTATUS;
    }

    public String getWORK_TYPE() {
        return WORK_TYPE;
    }

    public String getIMPRINT() {
        return IMPRINT;
    }

    public String getOPEN_ACCESS_JNL_TYPE_CODE() {
        return OPEN_ACCESS_JNL_TYPE_CODE;
    }

    public String getPRODUCT_WORK_ID() {
        return PRODUCT_WORK_ID;
    }

    public String getF_ACC_PROD_HIERARCHY() {
        return F_ACC_PROD_HIERARCHY;
    }

    public String getF_RESPONSIBILITY_CENTRE() {
        return F_RESPONSIBILITY_CENTRE;
    }

    public String getF_OPCO_R12() {
        return F_OPCO_R12;
    }

    public String getPRODUCT_WORK_PUB_DATE() {
        return PRODUCT_WORK_PUB_DATE;
    }

    public String getJOURNAL_ACRONYM() {
        return JOURNAL_ACRONYM;
    }

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
    public String f_accountable_product;

    public int getVOLUME() {
        return VOLUME;
    }

    public void setVOLUME(int VOLUME) {
        this.VOLUME = VOLUME;
    }

    public int getEDITION_NUMBER() {
        return EDITION_NUMBER;
    }

    public void setEDITION_NUMBER(int EDITION_NUMBER) {
        this.EDITION_NUMBER = EDITION_NUMBER;
    }

    public int getCOPYRIGHT_YEAR() {
        return COPYRIGHT_YEAR;
    }

    public void setCOPYRIGHT_YEAR(int COPYRIGHT_YEAR) {
        this.COPYRIGHT_YEAR = COPYRIGHT_YEAR;
    }

    public int VOLUME;
    public int EDITION_NUMBER;
    public int COPYRIGHT_YEAR;


    public int getPMX_SOURCE_REFERENCE() {
        return PMX_SOURCE_REFERENCE;
    }

    public void setPMX_SOURCE_REFERENCE(int PMX_SOURCE_REFERENCE) {
        this.PMX_SOURCE_REFERENCE = PMX_SOURCE_REFERENCE;
    }

    public int PMX_SOURCE_REFERENCE;

    public String getF_accountable_product() {
        return f_accountable_product;
    }

    public void setF_accountable_product(String f_accountable_product) {
        this.f_accountable_product = f_accountable_product;
    }

    public String getACC_PROD_ID() {
        return ACC_PROD_ID;
    }

    public void setACC_PROD_ID(String ACC_PROD_ID) {
        this.ACC_PROD_ID = ACC_PROD_ID;
    }

    public String getPARENT_ACC_PROD() {
        return PARENT_ACC_PROD;
    }

    public void setPARENT_ACC_PROD(String PARENT_ACC_PROD) {
        this.PARENT_ACC_PROD = PARENT_ACC_PROD;
    }

    public String ACC_PROD_ID;
    public String PARENT_ACC_PROD;


    public String getOWNERSHIP() {
        return OWNERSHIP;
    }

    public void setOWNERSHIP(String OWNERSHIP) {
        this.OWNERSHIP = OWNERSHIP;
    }

    public String OWNERSHIP;

    public String getF_TYPE() {
        return F_TYPE;
    }

    public void setF_TYPE(String f_TYPE) {
        F_TYPE = f_TYPE;
    }

    public String F_TYPE;

    public String getB_LOADID() {
        return B_LOADID;
    }

    public void setB_LOADID(String b_LOADID) {
        B_LOADID = b_LOADID;
    }

    public String B_LOADID;

    public String getF_EVENT() {
        return F_EVENT;
    }

    public void setF_EVENT(String f_EVENT) {
        F_EVENT = f_EVENT;
    }

    public String F_EVENT;

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

    public Integer getWorkCountEPHGD() {
        return workCountEPHGD;
    }

    public void setWorkCountEPHGD(Integer workCountEPHGD) {
        this.workCountEPHGD = workCountEPHGD;
    }

    public Integer workCountEPHGD;

    public Integer getWorkCountDQSTG() {
        return workCountDQSTG;
    }

    public void setWorkCountDQSTG(Integer workCountDQSTG) {
        this.workCountDQSTG = workCountDQSTG;
    }

    public Integer getWorkCountDQSTGnoError() {
        return workCountDQSTGnoError;
    }

    public void setWorkCountDQSTGnoError(Integer workCountDQSTGnoError) {
        this.workCountDQSTGnoError = workCountDQSTGnoError;
    }

    public Integer getErrorsCountEPH() {
        return errorsCountEPH;
    }

    public void setErrorsCountEPH(Integer errorsCountEPH) {
        this.errorsCountEPH = errorsCountEPH;
    }

    public Integer errorsCountEPH;

    public Integer workCountDQSTG;
    public Integer workCountDQSTGnoError;

    public String getRandom_value() {
        return random_value;
    }

    public void setRandom_value(String random_value) {
        this.random_value = random_value;
    }

    public String random_value;

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


    public String getIDENTIFER() {
        return IDENTIFER;
    }

    public void setIDENTIFER(String IDENTIFER) {
        this.IDENTIFER = IDENTIFER;
    }

    public String IDENTIFER;

    public String F_WWORK;

    public String getF_WWORK() {
        return F_WWORK;
    }

    public void setF_WWORK(String f_WWORK) {
        F_WWORK = f_WWORK;
    }

    @Override
    public String toString() {
        return "WorkDataObject{" +
                "PRIMARY_ISBN='" + PRIMARY_ISBN + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkDataObject that = (WorkDataObject) o;
        return Objects.equals(PRIMARY_ISBN, that.PRIMARY_ISBN) &&
                Objects.equals(WORK_TITLE, that.WORK_TITLE) &&
                Objects.equals(WORK_SUBTITLE, that.WORK_SUBTITLE) &&
                Objects.equals(PRODUCT_ID, that.PRODUCT_ID) &&
                Objects.equals(DAC_KEY, that.DAC_KEY) &&
                Objects.equals(PROJECT_NUM, that.PROJECT_NUM) &&
                Objects.equals(ISSN_L, that.ISSN_L) &&
                Objects.equals(JOURNAL_NUMBER, that.JOURNAL_NUMBER) &&
                Objects.equals(ELECTRONIC_RIGHTS_IND, that.ELECTRONIC_RIGHTS_IND) &&
                Objects.equals(BOOK_EDITION_NAME, that.BOOK_EDITION_NAME) &&
                Objects.equals(BOOK_VOLUME_NAME, that.BOOK_VOLUME_NAME) &&
                Objects.equals(PMC, that.PMC) &&
                Objects.equals(PMG, that.PMG) &&
                Objects.equals(WORK_ID, that.WORK_ID) &&
                Objects.equals(WORK_STATUS, that.WORK_STATUS) &&
                Objects.equals(WORK_SUBSTATUS, that.WORK_SUBSTATUS) &&
                Objects.equals(WORK_TYPE, that.WORK_TYPE) &&
                Objects.equals(IMPRINT, that.IMPRINT) &&
                Objects.equals(OPEN_ACCESS_JNL_TYPE_CODE, that.OPEN_ACCESS_JNL_TYPE_CODE) &&
                Objects.equals(PRODUCT_WORK_ID, that.PRODUCT_WORK_ID) &&
                Objects.equals(F_ACC_PROD_HIERARCHY, that.F_ACC_PROD_HIERARCHY) &&
                Objects.equals(F_RESPONSIBILITY_CENTRE, that.F_RESPONSIBILITY_CENTRE) &&
                Objects.equals(F_OPCO_R12, that.F_OPCO_R12) &&
                Objects.equals(PRODUCT_WORK_PUB_DATE.substring(0,10), that.PRODUCT_WORK_PUB_DATE.substring(0,10)) &&
                Objects.equals(JOURNAL_ACRONYM, that.JOURNAL_ACRONYM);
    }

    @Override
    public int hashCode() {
        return Objects.hash(PRIMARY_ISBN, WORK_TITLE, WORK_SUBTITLE, PRODUCT_ID, DAC_KEY, PROJECT_NUM, ISSN_L,
                JOURNAL_NUMBER, ELECTRONIC_RIGHTS_IND, BOOK_EDITION_NAME, BOOK_VOLUME_NAME, PMC, PMG, WORK_ID,
                WORK_STATUS, WORK_SUBSTATUS, WORK_TYPE, IMPRINT, OPEN_ACCESS_JNL_TYPE_CODE, PRODUCT_WORK_ID,
                F_ACC_PROD_HIERARCHY, F_RESPONSIBILITY_CENTRE, F_OPCO_R12, PRODUCT_WORK_PUB_DATE, JOURNAL_ACRONYM);
    }
}
