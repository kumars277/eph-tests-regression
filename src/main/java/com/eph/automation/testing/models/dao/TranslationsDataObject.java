package com.eph.automation.testing.models.dao;

public class TranslationsDataObject {

    public int stgCount;
    public int pmxCount;

    public int getStgCount() {
        return stgCount;
    }

    public void setStgCount(int stgCount) {
        this.stgCount = stgCount;
    }

    public int getPmxCount() {
        return pmxCount;
    }

    public void setPmxCount(int pmxCount) {
        this.pmxCount = pmxCount;
    }

    public int getSaCount() {
        return saCount;
    }

    public void setSaCount(int saCount) {
        this.saCount = saCount;
    }

    public int getGdCount() {
        return gdCount;
    }

    public void setGdCount(int gdCount) {
        this.gdCount = gdCount;
    }

    public int saCount;
    public int gdCount;

    public int getAeCount() {
        return aeCount;
    }

    public void setAeCount(int aeCount) {
        this.aeCount = aeCount;
    }

    public int aeCount;

    public String RELATIONSHIP_PMX_SOURCEREF;
    public String CHILD_PMX_SOURCE;
    public String PARENT_PMX_SOURCE;

    public String getRELATIONSHIP_PMX_SOURCEREF() {
        return RELATIONSHIP_PMX_SOURCEREF;
    }

    public void setRELATIONSHIP_PMX_SOURCEREF(String RELATIONSHIP_PMX_SOURCEREF) {
        this.RELATIONSHIP_PMX_SOURCEREF = RELATIONSHIP_PMX_SOURCEREF;
    }

    public String getCHILD_PMX_SOURCE() {
        return CHILD_PMX_SOURCE;
    }

    public void setCHILD_PMX_SOURCE(String CHILD_PMX_SOURCE) {
        this.CHILD_PMX_SOURCE = CHILD_PMX_SOURCE;
    }

    public String getPARENT_PMX_SOURCE() {
        return PARENT_PMX_SOURCE;
    }

    public void setPARENT_PMX_SOURCE(String PARENT_PMX_SOURCE) {
        this.PARENT_PMX_SOURCE = PARENT_PMX_SOURCE;
    }

    public String getF_RELATIONSHIP_TYPE() {
        return F_RELATIONSHIP_TYPE;
    }

    public void setF_RELATIONSHIP_TYPE(String f_RELATIONSHIP_TYPE) {
        F_RELATIONSHIP_TYPE = f_RELATIONSHIP_TYPE;
    }

    public String getEFFECTIVE_START_DATE() {
        return EFFECTIVE_START_DATE;
    }

    public void setEFFECTIVE_START_DATE(String EFFECTIVE_START_DATE) {
        this.EFFECTIVE_START_DATE = EFFECTIVE_START_DATE;
    }

    public String getWORK_REL_TRANSLATION_ID() {
        return WORK_REL_TRANSLATION_ID;
    }

    public void setWORK_REL_TRANSLATION_ID(String WORK_REL_TRANSLATION_ID) {
        this.WORK_REL_TRANSLATION_ID = WORK_REL_TRANSLATION_ID;
    }

    public String getB_CLASSNAME() {
        return B_CLASSNAME;
    }

    public void setB_CLASSNAME(String b_CLASSNAME) {
        B_CLASSNAME = b_CLASSNAME;
    }

    public String F_RELATIONSHIP_TYPE;
    public String EFFECTIVE_START_DATE;
    public String WORK_REL_TRANSLATION_ID;
    public String B_CLASSNAME;

    public String getENDON() {
        return ENDON;
    }

    public void setENDON(String ENDON) {
        this.ENDON = ENDON;
    }

    public String ENDON;

    public String getWorkID() {
        return workID;
    }

    public void setWorkID(String workID) {
        this.workID = workID;
    }

    public String workID;

    public String getTranslationId() {
        return translationId;
    }

    public void setTranslationId(String translationId) {
        this.translationId = translationId;
    }

    public String translationId;
}
