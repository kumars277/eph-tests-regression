package com.eph.automation.testing.models.dao;

import java.util.Objects;

/**
 * Created by Bistra Drazheva on 21/02/2019
 */
public class ProductRelationshipDataObject {
    private String RELATIONSHIP_PMX_SOURCEREF;
    private String OWNER_PMX_SOURCE;
    private String COMPONENT_PMX_SOURCE;
    private String F_RELATIONSHIP_TYPE;
    private String EFFECTIVE_START_DATE;
    private String ENDON;
    private String UPDATED;


    //SA
    private String F_EVENT;
    private String B_CLASSNAME;
    private String PRODUCT_REL_PACK_ID;
    private String F_PACKAGE_OWNER;
    private String F_COMPONENT;
    private String EFFECTIVE_END_DATE;


    public String getENDON() {
        return ENDON;
    }

    public void setENDON(String ENDON) {
        this.ENDON = ENDON;
    }

    public String getUPDATED() {
        return UPDATED;
    }

    public void setUPDATED(String UPDATED) {
        this.UPDATED = UPDATED;
    }

    public String getRELATIONSHIP_PMX_SOURCEREF() {
        return RELATIONSHIP_PMX_SOURCEREF;
    }

    public void setRELATIONSHIP_PMX_SOURCEREF(String RELATIONSHIP_PMX_SOURCEREF) {
        this.RELATIONSHIP_PMX_SOURCEREF = RELATIONSHIP_PMX_SOURCEREF;
    }

    public String getOWNER_PMX_SOURCE() {
        return OWNER_PMX_SOURCE;
    }

    public void setOWNER_PMX_SOURCE(String OWNER_PMX_SOURCE) {
        this.OWNER_PMX_SOURCE = OWNER_PMX_SOURCE;
    }

    public String getCOMPONENT_PMX_SOURCE() {
        return COMPONENT_PMX_SOURCE;
    }

    public void setCOMPONENT_PMX_SOURCE(String COMPONENT_PMX_SOURCE) {
        this.COMPONENT_PMX_SOURCE = COMPONENT_PMX_SOURCE;
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

    public String getEFFECTIVE_END_DATE() {
        return EFFECTIVE_END_DATE;
    }

    public void setEFFECTIVE_END_DATE(String EFFECTIVE_END_DATE) {
        this.EFFECTIVE_END_DATE = EFFECTIVE_END_DATE;
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

    public String getPRODUCT_REL_PACK_ID() {
        return PRODUCT_REL_PACK_ID;
    }

    public void setPRODUCT_REL_PACK_ID(String PRODUCT_REL_PACK_ID) {
        this.PRODUCT_REL_PACK_ID = PRODUCT_REL_PACK_ID;
    }

    public String getF_PACKAGE_OWNER() {
        return F_PACKAGE_OWNER;
    }

    public void setF_PACKAGE_OWNER(String f_PACKAGE_OWNER) {
        F_PACKAGE_OWNER = f_PACKAGE_OWNER;
    }

    public String getF_COMPONENT() {
        return F_COMPONENT;
    }

    public void setF_COMPONENT(String f_COMPONENT) {
        F_COMPONENT = f_COMPONENT;
    }


    @Override
    public String toString() {
        return "ProductRelationshipDataObject{" +
                "RELATIONSHIP_PMX_SOURCEREF='" + RELATIONSHIP_PMX_SOURCEREF + '\'' +
                ", OWNER_PMX_SOURCE='" + OWNER_PMX_SOURCE + '\'' +
                ", COMPONENT_PMX_SOURCE='" + COMPONENT_PMX_SOURCE + '\'' +
                ", F_RELATIONSHIP_TYPE='" + F_RELATIONSHIP_TYPE + '\'' +
                ", EFFECTIVE_START_DATE='" + EFFECTIVE_START_DATE + '\'' +
                ", ENDON='" + ENDON + '\'' +
                ", F_EVENT='" + F_EVENT + '\'' +
                ", B_CLASSNAME='" + B_CLASSNAME + '\'' +
                ", PRODUCT_REL_PACK_ID='" + PRODUCT_REL_PACK_ID + '\'' +
                ", F_PACKAGE_OWNER='" + F_PACKAGE_OWNER + '\'' +
                ", F_COMPONENT='" + F_COMPONENT + '\'' +
                ", EFFECTIVE_END_DATE='" + EFFECTIVE_END_DATE + '\'' +
                '}';
    }
}
