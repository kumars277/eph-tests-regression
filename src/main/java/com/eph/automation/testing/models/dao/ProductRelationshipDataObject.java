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

    //SA
    private String F_EVENT;
    private String B_CLASSNAME;
    private String PRODUCT_REL_PACKAGE_ID;
    private String F_PACKAGE_OWNER;
    private String F_COMPONENT;
    private String EFFECTIVE_END_DATE;


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

    public String getPRODUCT_REL_PACKAGE_ID() {
        return PRODUCT_REL_PACKAGE_ID;
    }

    public void setPRODUCT_REL_PACKAGE_ID(String PRODUCT_REL_PACKAGE_ID) {
        this.PRODUCT_REL_PACKAGE_ID = PRODUCT_REL_PACKAGE_ID;
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProductRelationshipDataObject that = (ProductRelationshipDataObject) o;
        return Objects.equals(RELATIONSHIP_PMX_SOURCEREF, that.RELATIONSHIP_PMX_SOURCEREF) &&
                Objects.equals(OWNER_PMX_SOURCE, that.OWNER_PMX_SOURCE) &&
                Objects.equals(COMPONENT_PMX_SOURCE, that.COMPONENT_PMX_SOURCE) &&
                F_RELATIONSHIP_TYPE.equals(that.F_RELATIONSHIP_TYPE) &&
                Objects.equals(EFFECTIVE_START_DATE, that.EFFECTIVE_START_DATE) &&
                F_EVENT.equals(that.F_EVENT) &&
                B_CLASSNAME.equals(that.B_CLASSNAME) &&
                PRODUCT_REL_PACKAGE_ID.equals(that.PRODUCT_REL_PACKAGE_ID) &&
                Objects.equals(F_PACKAGE_OWNER, that.F_PACKAGE_OWNER) &&
                F_COMPONENT.equals(that.F_COMPONENT) &&
                Objects.equals(EFFECTIVE_END_DATE, that.EFFECTIVE_END_DATE);
    }


}
