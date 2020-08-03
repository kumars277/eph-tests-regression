package com.eph.automation.testing.models.dao.JRBIDataLakeAccess;
//package com.eph.automation.testing.models.api;

public class JRBIWorkExtJson {

    private String primarySiteSystem;

    public String getPrimarySiteSystem() {
        return primarySiteSystem;
    }

    public void setPrimarySiteSystem(String primarySiteSystem) {
        this.primarySiteSystem = primarySiteSystem;
    }

    private String primarySiteAcronym;

    public String getPrimarySiteAcronym() {
        return primarySiteAcronym;
    }

    public void setPrimarySiteAcronym(String primarySiteAcronym) {
        this.primarySiteAcronym = primarySiteAcronym;
    }

    private String primarySiteSupportLevel;

    public String getPrimarySiteSupportLevel() {
        return primarySiteSupportLevel;
    }

    public void setPrimarySiteSupportLevel(String primarySiteSupportLevel) {
        this.primarySiteSupportLevel = primarySiteSupportLevel;
    }


    private String issueProdTypeCode;

    public String getIssueProdTypeCode() {
        return issueProdTypeCode;
    }

    public void setIssueProdTypeCode(String issueProdTypeCode) {
        this.issueProdTypeCode = issueProdTypeCode;
    }

    private String catalogueVolumesQty;

    public void setCatalogueVolumesQty(String catalogueVolumesQty) {
        this.catalogueVolumesQty = catalogueVolumesQty;
    }

    public String getCatalogueVolumesQty() {
        return catalogueVolumesQty;
    }

    private String catalogueIssuesQty;

    public void setCatalogueIssuesQty(String catalogueIssuesQty) {
        this.catalogueIssuesQty = catalogueIssuesQty;
    }

    public String getCatalogueIssuesQty() {
        return catalogueIssuesQty;
    }

    private String catalogueVolumeFrom;

    public String getCatalogueVolumeFrom() {
        return catalogueVolumeFrom;
    }

    public void setCatalogueVolumeFrom(String catalogueVolumeFrom) {
        this.catalogueVolumeFrom = catalogueVolumeFrom;
    }

    private String catalogueVolumeTo;

    public void setCatalogueVolumeTo(String catalogueVolumeTo) {
        this.catalogueVolumeTo = catalogueVolumeTo;
    }

    public String getCatalogueVolumeTo() {
        return catalogueVolumeTo;
    }

    private String rfIssuesQty;

    public String getRfIssuesQty() {
        return rfIssuesQty;
    }

    public void setRfIssuesQty(String rfIssuesQty) {
        this.rfIssuesQty = rfIssuesQty;
    }

    private String rfTotalPagesQty;

    public String getRfTotalPagesQty() {
        return rfTotalPagesQty;
    }

    public void setRfTotalPagesQty(String rfTotalPagesQty) {
        this.rfTotalPagesQty = rfTotalPagesQty;
    }

    private String rfFvi;

    public String getRfFvi() {
        return rfFvi;
    }

    public void setRfFvi(String rfFvi) {
        this.rfFvi = rfFvi;
    }

    private String rfLvi;

    public void setRfLvi(String rfLvi) {
        this.rfLvi = rfLvi;
    }

    public String getRfLvi() {
        return rfLvi;
    }

    private String ptsBusinessUnitDesc;

    public String getPtsBusinessUnitDesc() {
        return ptsBusinessUnitDesc;
    }

    public void setPtsBusinessUnitDesc(String ptsBusinessUnitDesc) {
        this.ptsBusinessUnitDesc = ptsBusinessUnitDesc;
    }

    private JRBIPersonExtJson[] workExtendedPersons;
    public JRBIPersonExtJson[] getWorkExtendedPersons() {return workExtendedPersons;}
    public void setWorkExtendedPersons(JRBIPersonExtJson[] workExtendedPersons) {this.workExtendedPersons = workExtendedPersons;}
}

