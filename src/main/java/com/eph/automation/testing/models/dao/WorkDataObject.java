package com.eph.automation.testing.models.dao;

import java.util.Objects;

/**
 * Created by RAVIVARMANS on 26/11/2018.
 * updated by Nishant @ 9 Apr 2020
 */
public class WorkDataObject {


    private String WORK_ID;
    private String F_WWORK;
    private String B_CLASSNAME;
    private String UPDATED;
    private String EXTERNAL_REFERENCE;
    private String WORK_TITLE;
    private String WORK_SUBTITLE;
    private String ELECTRONIC_RIGHTS_IND;
    private String BOOK_VOLUME_NAME;
    private String VOLUME;
    private String COPYRIGHT_YEAR;
    private String BOOK_EDITION_NAME;
    private String EDITION_NUMBER;
    private String PLANNED_LAUNCH_DATE;
    private String ACTUAL_LAUNCH_DATE;
    private String PLANNED_DISCONTINUE_DATE;
    private String ACTUAL_DISCONTINUE_DATE;
    private String SUMMARY_CHANGED;
    private String EVENT_DESCRIPTION;
    private String F_TYPE;
    private String WORK_TYPE;
    private String WORK_SUBSTATUS;
    private String WORK_STATUS;
    private String f_accountable_product;
    private String PMC;
    private String OPEN_ACCESS_JNL_TYPE_CODE;
    private String OPEN_ACCESS_TYPE;
    private String IMPRINT;
    private String LEGAL_OWNERSHIP;
    private String SOCIETY_OWNERSHIP;
    private String SUBSCRIPTION_TYPE;
    private String LANGUAGE_CODE;
    private String F_EVENT;

    private String PRODUCT_ID;
    private String PRODUCT_WORK_ID;
    private String PRODUCT_WORK_PUB_DATE;
    private String PARENT_ACC_PROD;
    private String PROJECT_NUM;
    private String IDENTIFIER;
    private String PRIMARY_ISBN;
    private String ISSN_L;
    private String DAC_KEY;
    private String JOURNAL_NUMBER;
    private String JOURNAL_ACRONYM;
    private String F_ACC_PROD_HIERARCHY;
    private String F_RESPONSIBILITY_CENTRE;
    private String F_OPCO_R12;
    private String B_LOADID;
    private String SA;
    private String STG;
    private String RECORD_END_DATE;
    private int PMX_SOURCE_REFERENCE;
    private String OWNERSHIP;
    private String PMG;

    public String getIDENTIFIER() {return IDENTIFIER;}
    public void setIDENTIFIER(String IDENTIFIER) {this.IDENTIFIER = IDENTIFIER;}


    //for Data Model Changes: created by Nishant @ 11 Jun 2020 for
    public String getPLANNED_LAUNCH_DATE() {return PLANNED_LAUNCH_DATE;}
    public void setPLANNED_LAUNCH_DATE(String PLANNED_LAUNCH_DATE) {this.PLANNED_LAUNCH_DATE = PLANNED_LAUNCH_DATE;}

    public String getLEGAL_OWNERSHIP() {return LEGAL_OWNERSHIP;}
    public void setLEGAL_OWNERSHIP(String LEGAL_OWNERSHIP) {this.LEGAL_OWNERSHIP = LEGAL_OWNERSHIP;}



    public String getPRODUCT_ID() {return PRODUCT_ID;}
    public void setPRODUCT_ID(String PRODUCT_ID) {this.PRODUCT_ID = PRODUCT_ID;}

    public String getWORK_TITLE() {return WORK_TITLE;}
    public void setWORK_TITLE(String WORK_TITLE) {this.WORK_TITLE = WORK_TITLE;}

    public String getELECTRONIC_RIGHTS_IND() {
        if(ELECTRONIC_RIGHTS_IND!=null){
        if(ELECTRONIC_RIGHTS_IND.equalsIgnoreCase("t"))ELECTRONIC_RIGHTS_IND= "True";
        if(ELECTRONIC_RIGHTS_IND.equalsIgnoreCase("f")) ELECTRONIC_RIGHTS_IND= "False";}
        return ELECTRONIC_RIGHTS_IND;}

    public void setELECTRONIC_RIGHTS_IND(String ELECTRONIC_RIGHTS_IND) {this.ELECTRONIC_RIGHTS_IND = ELECTRONIC_RIGHTS_IND;}

    public String getLANGUAGE_CODE() {return LANGUAGE_CODE;}
    public void setLANGUAGE_CODE(String LANGUAGE_CODE) {this.LANGUAGE_CODE = LANGUAGE_CODE;}

    public String getSUBSCRIPTION_TYPE() {return SUBSCRIPTION_TYPE;}
    public void setSUBSCRIPTION_TYPE(String SUBSCRIPTION_TYPE) {this.SUBSCRIPTION_TYPE = SUBSCRIPTION_TYPE;}

    public String getPRIMARY_ISBN() {return PRIMARY_ISBN;}
    public void setPRIMARY_ISBN(String PRIMARY_ISBN) {this.PRIMARY_ISBN = PRIMARY_ISBN;}

    public String getWORK_SUBTITLE() {return WORK_SUBTITLE;}
    public void setWORK_SUBTITLE(String WORK_SUBTITLE) {this.WORK_SUBTITLE = WORK_SUBTITLE;}

    public String getDAC_KEY() {return DAC_KEY;}
    public void setDAC_KEY(String DAC_KEY) {this.DAC_KEY = DAC_KEY;}

    public String getPROJECT_NUM() {return PROJECT_NUM;}
    public void setPROJECT_NUM(String PROJECT_NUM) {this.PROJECT_NUM = PROJECT_NUM;}

    public String getISSN_L() {return ISSN_L;}
    public void setISSN_L(String ISSN_L) {this.ISSN_L = ISSN_L;}

    public String getJOURNAL_NUMBER() {return JOURNAL_NUMBER;}
    public void setJOURNAL_NUMBER(String JOURNAL_NUMBER) {this.JOURNAL_NUMBER = JOURNAL_NUMBER;}

    public String getBOOK_EDITION_NAME() {return BOOK_EDITION_NAME;}
    public void setBOOK_EDITION_NAME(String BOOK_EDITION_NAME) {this.BOOK_EDITION_NAME = BOOK_EDITION_NAME;}

    public String getBOOK_VOLUME_NAME() {return BOOK_VOLUME_NAME;}
    public void setBOOK_VOLUME_NAME(String BOOK_VOLUME_NAME) {this.BOOK_VOLUME_NAME = BOOK_VOLUME_NAME;}

    public String getPMC() {return PMC;}
    public void setPMC(String PMC) {this.PMC = PMC;}

    public String getPMG() {return PMG;}
    public void setPMG(String PMG) {this.PMG = PMG;}

    public String getWORK_ID() {return WORK_ID;}
    public void setWORK_ID(String WORK_ID) {this.WORK_ID = WORK_ID;}

    public String getWORK_STATUS() {return WORK_STATUS;}
    public void setWORK_STATUS(String WORK_STATUS) {this.WORK_STATUS = WORK_STATUS;}

    public String getWORK_SUBSTATUS() {return WORK_SUBSTATUS;}
    public void setWORK_SUBSTATUS(String WORK_SUBSTATUS) {this.WORK_SUBSTATUS = WORK_SUBSTATUS;}

    public String getWORK_TYPE() {return WORK_TYPE;}
    public void setWORK_TYPE(String WORK_TYPE) {this.WORK_TYPE = WORK_TYPE;}

    public String getIMPRINT() {return IMPRINT;}
    public void setIMPRINT(String IMPRINT) {this.IMPRINT = IMPRINT;}

    public String getOPEN_ACCESS_JNL_TYPE_CODE() {return OPEN_ACCESS_JNL_TYPE_CODE;}
    public void setOPEN_ACCESS_JNL_TYPE_CODE(String OPEN_ACCESS_JNL_TYPE_CODE) {this.OPEN_ACCESS_JNL_TYPE_CODE = OPEN_ACCESS_JNL_TYPE_CODE;}

    public String getOPEN_ACCESS_TYPE() {return OPEN_ACCESS_TYPE;}
    public void setOPEN_ACCESS_TYPE(String OPEN_ACCESS_TYPE) {this.OPEN_ACCESS_TYPE = OPEN_ACCESS_TYPE;}

    public String getPRODUCT_WORK_ID() {return PRODUCT_WORK_ID;}
    public void setPRODUCT_WORK_ID(String PRODUCT_WORK_ID) {this.PRODUCT_WORK_ID = PRODUCT_WORK_ID;}

    public String getF_ACC_PROD_HIERARCHY() {return F_ACC_PROD_HIERARCHY;}
    public void setF_ACC_PROD_HIERARCHY(String f_ACC_PROD_HIERARCHY) {F_ACC_PROD_HIERARCHY = f_ACC_PROD_HIERARCHY;}

    public String getF_RESPONSIBILITY_CENTRE() {return F_RESPONSIBILITY_CENTRE;}
    public void setF_RESPONSIBILITY_CENTRE(String f_RESPONSIBILITY_CENTRE) {F_RESPONSIBILITY_CENTRE = f_RESPONSIBILITY_CENTRE;}

    public String getF_OPCO_R12() {return F_OPCO_R12;}
    public void setF_OPCO_R12(String f_OPCO_R12) {F_OPCO_R12 = f_OPCO_R12;}

    public String getPRODUCT_WORK_PUB_DATE() {return PRODUCT_WORK_PUB_DATE;}
    public void setPRODUCT_WORK_PUB_DATE(String PRODUCT_WORK_PUB_DATE) {this.PRODUCT_WORK_PUB_DATE = PRODUCT_WORK_PUB_DATE;}

    public String getJOURNAL_ACRONYM() {return JOURNAL_ACRONYM;}
    public void setJOURNAL_ACRONYM(String JOURNAL_ACRONYM) {this.JOURNAL_ACRONYM = JOURNAL_ACRONYM;}

    public String getSA() {return SA;}
    public void setSA(String SA) {this.SA = SA;}

    public String getSTG() {return STG;}
    public void setSTG(String STG) {this.STG = STG;}

    public String getRECORD_END_DATE() {return RECORD_END_DATE;}
    public void setRECORD_END_DATE(String RECORD_END_DATE) {this.RECORD_END_DATE = RECORD_END_DATE;}

    public String getUPDATED() {return UPDATED;}
    public void setUPDATED(String UPDATED) {this.UPDATED = UPDATED;}

    public String getVOLUME() {return VOLUME;}
    public void setVOLUME(String VOLUME) {this.VOLUME = VOLUME;}

    public String getEDITION_NUMBER() {return EDITION_NUMBER;}
    public void setEDITION_NUMBER(String EDITION_NUMBER) {this.EDITION_NUMBER = EDITION_NUMBER;}

    public String getCOPYRIGHT_YEAR() {return COPYRIGHT_YEAR;}
    public void setCOPYRIGHT_YEAR(String COPYRIGHT_YEAR) {this.COPYRIGHT_YEAR = COPYRIGHT_YEAR;}

    public int getPMX_SOURCE_REFERENCE() {return PMX_SOURCE_REFERENCE;}
    public void setPMX_SOURCE_REFERENCE(int PMX_SOURCE_REFERENCE) {this.PMX_SOURCE_REFERENCE = PMX_SOURCE_REFERENCE;}

    public String getF_accountable_product() {return f_accountable_product;}
    public void setF_accountable_product(String f_accountable_product) {this.f_accountable_product = f_accountable_product;}


    public String getPARENT_ACC_PROD() {return PARENT_ACC_PROD;}
    public void setPARENT_ACC_PROD(String PARENT_ACC_PROD) {this.PARENT_ACC_PROD = PARENT_ACC_PROD;}

    public String getOWNERSHIP() {return OWNERSHIP;}
    public void setOWNERSHIP(String OWNERSHIP) {this.OWNERSHIP = OWNERSHIP;}

    public String getF_TYPE() {return F_TYPE;}
    public void setF_TYPE(String f_TYPE) {F_TYPE = f_TYPE;}

    public String getB_LOADID() {return B_LOADID;}
    public void setB_LOADID(String b_LOADID) {B_LOADID = b_LOADID;}

    public String getF_EVENT() {return F_EVENT;}
    public void setF_EVENT(String f_EVENT) {F_EVENT = f_EVENT;}

    public String getB_CLASSNAME() {return B_CLASSNAME;}
    public void setB_CLASSNAME(String b_CLASSNAME) {B_CLASSNAME = b_CLASSNAME;}

    public String getF_WWORK() {return F_WWORK;}
    public void setF_WWORK(String f_WWORK) {F_WWORK = f_WWORK;}

    @Override
    public String toString() {return "WorkDataObject{PRIMARY_ISBN='" + PRIMARY_ISBN + '\'' +'}';}

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
                Objects.equals(OPEN_ACCESS_TYPE, that.OPEN_ACCESS_TYPE) &&
                Objects.equals(PRODUCT_WORK_ID, that.PRODUCT_WORK_ID) &&
                Objects.equals(F_ACC_PROD_HIERARCHY, that.F_ACC_PROD_HIERARCHY) &&
                Objects.equals(F_RESPONSIBILITY_CENTRE, that.F_RESPONSIBILITY_CENTRE) &&
                Objects.equals(F_OPCO_R12, that.F_OPCO_R12) &&
                Objects.equals(PRODUCT_WORK_PUB_DATE.substring(0, 10), that.PRODUCT_WORK_PUB_DATE.substring(0, 10)) &&
                Objects.equals(JOURNAL_ACRONYM, that.JOURNAL_ACRONYM);
    }

    @Override
    public int hashCode() {
        return Objects.hash(PRIMARY_ISBN, WORK_TITLE, WORK_SUBTITLE, PRODUCT_ID, DAC_KEY, PROJECT_NUM, ISSN_L,
                JOURNAL_NUMBER, ELECTRONIC_RIGHTS_IND, BOOK_EDITION_NAME, BOOK_VOLUME_NAME, PMC, PMG, WORK_ID,
                WORK_STATUS, WORK_SUBSTATUS, WORK_TYPE, IMPRINT, OPEN_ACCESS_TYPE, PRODUCT_WORK_ID,
                F_ACC_PROD_HIERARCHY, F_RESPONSIBILITY_CENTRE, F_OPCO_R12, PRODUCT_WORK_PUB_DATE, JOURNAL_ACRONYM);
    }


}
