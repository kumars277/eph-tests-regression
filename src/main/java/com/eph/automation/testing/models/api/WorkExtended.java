package com.eph.automation.testing.models.api;

import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.web.steps.ApiWorksSearchSteps;
import org.junit.Assert;

/**
 * created by Nishant @ 19 Jun 2020
 * for JRBI data reflect in APIv3 work extended, person extended and manifestation extended
 */

public class WorkExtended {

    private String primarySiteSystem;

    public String getPrimarySiteSystem() {
        if(primarySiteSystem==null) return "";
        else return primarySiteSystem;
    }

    public void setPrimarySiteSystem(String primarySiteSystem) {
        this.primarySiteSystem = primarySiteSystem;
    }

    private String primarySiteAcronym;

    public String getPrimarySiteAcronym() {
        if(primarySiteAcronym==null) return "";
        else return primarySiteAcronym;
    }

    public void setPrimarySiteAcronym(String primarySiteAcronym) {
        this.primarySiteAcronym = primarySiteAcronym;
    }

    private String primarySiteSupportLevel;

    public String getPrimarySiteSupportLevel() {
        if(primarySiteSupportLevel==null) return "";
        else return primarySiteSupportLevel;
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
        if(catalogueVolumesQty==null) return "";
        else return catalogueVolumesQty;

    }

    private String catalogueIssuesQty="";

    public void setCatalogueIssuesQty(String catalogueIssuesQty) {
        this.catalogueIssuesQty = catalogueIssuesQty;
    }

    public String getCatalogueIssuesQty() {
        return catalogueIssuesQty;
    }

    private String catalogueVolumeFrom="";

    public String getCatalogueVolumeFrom() {
        return catalogueVolumeFrom;
    }

    public void setCatalogueVolumeFrom(String catalogueVolumeFrom) {
        this.catalogueVolumeFrom = catalogueVolumeFrom;
    }

    private String catalogueVolumeTo="";

    public void setCatalogueVolumeTo(String catalogueVolumeTo) {
        this.catalogueVolumeTo = catalogueVolumeTo;
    }

    public String getCatalogueVolumeTo() {
        return catalogueVolumeTo;
    }

    private String rfIssuesQty="";

    public String getRfIssuesQty() {
        return rfIssuesQty;
    }

    public void setRfIssuesQty(String rfIssuesQty) {
        this.rfIssuesQty = rfIssuesQty;
    }

    private String rfTotalPagesQty="";

    public String getRfTotalPagesQty() {
        return rfTotalPagesQty;
    }

    public void setRfTotalPagesQty(String rfTotalPagesQty) {
        this.rfTotalPagesQty = rfTotalPagesQty;
    }

    private String rfFvi="";

    public String getRfFvi() {
        return rfFvi;
    }

    public void setRfFvi(String rfFvi) {
        this.rfFvi = rfFvi;
    }

    private String rfLvi="";

    public void setRfLvi(String rfLvi) {
        this.rfLvi = rfLvi;
    }

    public String getRfLvi() {
        return rfLvi;
    }

    private String ptsBusinessUnitDesc="";

    public String getPtsBusinessUnitDesc() {
        return ptsBusinessUnitDesc;
    }

    public void setPtsBusinessUnitDesc(String ptsBusinessUnitDesc) {
        this.ptsBusinessUnitDesc = ptsBusinessUnitDesc;
    }

    private WorkExtendedPersons[] workExtendedPersons;

    public WorkExtendedPersons[] getWorkExtendedPersons() {
        return workExtendedPersons;
    }

    public void setWorkExtendedPersons(WorkExtendedPersons[] workExtendedPersons) {
        this.workExtendedPersons = workExtendedPersons;
    }


    public void compareWithDB()
    {
        Assert.assertEquals(primarySiteSystem, DataQualityContext.workExtendedTestClass.getWorkExtended().getPrimarySiteSystem());
        printLog("primarySiteSystem");

        Assert.assertEquals(primarySiteAcronym, DataQualityContext.workExtendedTestClass.getWorkExtended().getPrimarySiteAcronym());
        printLog("primarySiteAcronym");

        Assert.assertEquals(primarySiteSupportLevel, DataQualityContext.workExtendedTestClass.getWorkExtended().getPrimarySiteSupportLevel());
        printLog("primarySiteSupportLevel");

        Assert.assertEquals(issueProdTypeCode, DataQualityContext.workExtendedTestClass.getWorkExtended().getIssueProdTypeCode());
        printLog("issueProdTypeCode");

        Assert.assertEquals(catalogueVolumesQty, DataQualityContext.workExtendedTestClass.getWorkExtended().getCatalogueVolumesQty());
        printLog("catalogueVolumesQty");

        Assert.assertEquals(catalogueIssuesQty, DataQualityContext.workExtendedTestClass.getWorkExtended().getCatalogueIssuesQty());
        printLog("catalogueIssuesQty");

        Assert.assertEquals(catalogueVolumeFrom, DataQualityContext.workExtendedTestClass.getWorkExtended().getCatalogueVolumeFrom());
        printLog("catalogueVolumeFrom");

        Assert.assertEquals(catalogueVolumeTo, DataQualityContext.workExtendedTestClass.getWorkExtended().getCatalogueVolumeTo());
        printLog("catalogueVolumeTo");

        Assert.assertEquals(rfIssuesQty, DataQualityContext.workExtendedTestClass.getWorkExtended().getRfIssuesQty());
        printLog("rfIssuesQty");

        Assert.assertEquals(rfTotalPagesQty, DataQualityContext.workExtendedTestClass.getWorkExtended().getRfTotalPagesQty());
        printLog("rfTotalPagesQty");

        Assert.assertEquals(rfFvi, DataQualityContext.workExtendedTestClass.getWorkExtended().getRfFvi());
        printLog("rfFvi");

        Assert.assertEquals(rfLvi, DataQualityContext.workExtendedTestClass.getWorkExtended().getRfLvi());
        printLog("rfLvi");

        Assert.assertEquals(ptsBusinessUnitDesc, DataQualityContext.workExtendedTestClass.getWorkExtended().getPtsBusinessUnitDesc());
        printLog("ptsBusinessUnitDesc");

       if(workExtendedPersons!=null) {
           WorkExtendedPersons[] wep = workExtendedPersons.clone();
           WorkExtendedPersons[] wepDB = DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedPersons.clone();

           for (int i = 0; i < workExtendedPersons.length; i++) {
               Log.info("verifying...workExtendedPerson..." + i);
               Assert.assertEquals(wep[i].getExtendedRole().get("code"), wepDB[i].getExtendedRole().get("code"));
               printLog("extendedRole code");
               Assert.assertEquals(wep[i].getExtendedRole().get("name"), wepDB[i].getExtendedRole().get("name"));
               printLog("extendedRole name");
               Assert.assertEquals(wep[i].getExtendedPerson().get("firstName"), wepDB[i].getExtendedPerson().get("firstName"));
               printLog("extendedPerson firstName");
               Assert.assertEquals(wep[i].getExtendedPerson().get("lastName"), wepDB[i].getExtendedPerson().get("lastName"));
               printLog("extendedPerson lastName");
               Assert.assertEquals(wep[i].getExtendedPerson().get("peoplehubId"), wepDB[i].getExtendedPerson().get("peoplehubId"));
               printLog("extendedPerson peoplehubId");
               Assert.assertEquals(wep[i].getExtendedPerson().get("email"), wepDB[i].getExtendedPerson().get("email"));
               printLog("extendedPerson email");
           }
       }
    }

    private void printLog(String verified){Log.info("verified..."+verified);}

}
