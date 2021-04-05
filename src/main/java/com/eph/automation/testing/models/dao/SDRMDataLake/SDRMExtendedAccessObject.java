package com.eph.automation.testing.models.dao.SDRMDataLake;

import com.sun.xml.internal.ws.api.addressing.AddressingVersion;

public class SDRMExtendedAccessObject {

    private String EPR_ID;
    private String PRODUCT_TYPE;
    private String LAST_UPDATED_DATE;
    private String AVAILABILITY_FORMAT;
    private String AVAILABILITY_START_DATE;
    private String DELETE_FLAG;

    private String AVAILABILITY_STATUS;

    public String getAVAILABILITY_STATUS()
    {
        return AVAILABILITY_STATUS;
    }

    public void setAVAILABILITY_STATUS(String AVAILABILITY_STATUS)
    {
        this.AVAILABILITY_STATUS=AVAILABILITY_STATUS;
    }

    public String getDELETE_FLAG()
    {
        return DELETE_FLAG;
    }

    public void setDELETE_FLAG(String DELETE_FLAG)
    {
        this.DELETE_FLAG=DELETE_FLAG;
    }


    public String getAVAILABILITY_START_DATE()
    {
        return AVAILABILITY_START_DATE;
    }

    public void setAVAILABILITY_START_DATE(String AVAILABILITY_START_DATE)
    {
        this.AVAILABILITY_START_DATE=AVAILABILITY_START_DATE;
    }


    public String getAVAILABILITY_FORMAT()
    {
        return AVAILABILITY_FORMAT;
    }

    public void setAVAILABILITY_FORMAT(String AVAILABILITY_FORMAT)
    {
        this.AVAILABILITY_FORMAT=AVAILABILITY_FORMAT;
    }

    public String getLAST_UPDATED_DATE()
    {
        return LAST_UPDATED_DATE;
    }

    public void setLAST_UPDATED_DATE(String LAST_UPDATED_DATE)
    {
        this.LAST_UPDATED_DATE=LAST_UPDATED_DATE;
    }


    public String getEPR_ID() {
        return EPR_ID;
    }
    public void setEPR_ID(String EPR_ID) {
        this.EPR_ID = EPR_ID;
    }

    public String getPRODUCT_TYPE() {
        return PRODUCT_TYPE;
    }
    public void setPRODUCT_TYPE(String PRODUCT_TYPE) {
        this.PRODUCT_TYPE = PRODUCT_TYPE;
    }


}
