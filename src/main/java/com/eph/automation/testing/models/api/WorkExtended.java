package com.eph.automation.testing.models.api;

import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.web.steps.ApiWorksSearchSteps;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.Assert;

import java.util.HashMap;

/**
 * created by Nishant @ 19 Jun 2020
 * for JRBI data reflect in APIv3 work extended, person extended and manifestation extended
 * updated by Nishant @ 08 Feb 2021, EPHD-2747
 * updated by Nishant @ 01 Mar 2021 EPHD-2898
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkExtended {

    private String journalElsComInd;
    public String getJournalElsComInd() {if(journalElsComInd==null) return "";else return journalElsComInd;}
    public void setJournalElsComInd(String journalElsComInd) {this.journalElsComInd = journalElsComInd;}

    private String journalAimsScope;
    public String getJournalAimsScope() {if(journalAimsScope==null) return "";else return journalAimsScope;}
    public void setJournalAimsScope(String journalAimsScope) {this.journalAimsScope = journalAimsScope;}

    private String journalProdSiteCode;
    public String getJournalProdSiteCode() {return journalProdSiteCode;}
    public void setJournalProdSiteCode(String journalProdSiteCode) {this.journalProdSiteCode = journalProdSiteCode;}

    private String imageFileRef;
    public String getImageFileRef() {if(imageFileRef==null) return "";else return imageFileRef;}
    public void setImageFileRef(String imageFileRef) {this.imageFileRef = imageFileRef;}

    private String masterISBN;
    public String getMasterISBN() {return masterISBN;}
    public void setMasterISBN(String masterISBN) {this.masterISBN = masterISBN;}

    private String authorByLineText;
    public String getAuthorByLineText() {return authorByLineText;}
    public void setAuthorByLineText(String authorByLineText) {this.authorByLineText = authorByLineText;}

    private String internalElsDiv;
    public String getInternalElsDiv() {return internalElsDiv;}
    public void setInternalElsDiv(String internalElsDiv) {this.internalElsDiv = internalElsDiv;}

    private String profitCentre;
    public String getProfitCentre() {return profitCentre;}
    public void setProfitCentre(String profitCentre) {this.profitCentre = profitCentre;}

    private String textRefTrade;
    public String getTextRefTrade() {return textRefTrade;}
    public void setTextRefTrade(String textRefTrade) {this.textRefTrade = textRefTrade;}

    private String primarySiteSystem;
    public String getPrimarySiteSystem() {if(primarySiteSystem==null) return "";else return primarySiteSystem;}
    public void setPrimarySiteSystem(String primarySiteSystem) {this.primarySiteSystem = primarySiteSystem;}

    private String primarySiteAcronym;
    public String getPrimarySiteAcronym() {if(primarySiteAcronym==null) return "";else return primarySiteAcronym;}
    public void setPrimarySiteAcronym(String primarySiteAcronym) {this.primarySiteAcronym = primarySiteAcronym;}

    private String primarySiteSupportLevel;
    public String getPrimarySiteSupportLevel() {if(primarySiteSupportLevel==null) return "";else return primarySiteSupportLevel;}
    public void setPrimarySiteSupportLevel(String primarySiteSupportLevel) {this.primarySiteSupportLevel = primarySiteSupportLevel;}

    private String issueProdTypeCode;
    public String getIssueProdTypeCode() {if(issueProdTypeCode==null) return "";else return issueProdTypeCode;}
    public void setIssueProdTypeCode(String issueProdTypeCode) {this.issueProdTypeCode = issueProdTypeCode;}

    private String catalogueVolumesQty;
    public void setCatalogueVolumesQty(String catalogueVolumesQty) {this.catalogueVolumesQty = catalogueVolumesQty;}
    public String getCatalogueVolumesQty() {if(catalogueVolumesQty==null) return "";else return catalogueVolumesQty;}

    private String catalogueIssuesQty="";
    public void setCatalogueIssuesQty(String catalogueIssuesQty) {this.catalogueIssuesQty = catalogueIssuesQty;}
    public String getCatalogueIssuesQty() {return catalogueIssuesQty;}

    private String catalogueVolumeFrom="";
    public String getCatalogueVolumeFrom() {return catalogueVolumeFrom;}
    public void setCatalogueVolumeFrom(String catalogueVolumeFrom) {this.catalogueVolumeFrom = catalogueVolumeFrom;}

    private String catalogueVolumeTo="";
    public void setCatalogueVolumeTo(String catalogueVolumeTo) {this.catalogueVolumeTo = catalogueVolumeTo;}
    public String getCatalogueVolumeTo() {return catalogueVolumeTo;}

    private String rfIssuesQty="";
    public String getRfIssuesQty() {return rfIssuesQty;}
    public void setRfIssuesQty(String rfIssuesQty) {this.rfIssuesQty = rfIssuesQty;}

    private String rfTotalPagesQty="";
    public String getRfTotalPagesQty() {return rfTotalPagesQty;}
    public void setRfTotalPagesQty(String rfTotalPagesQty) {this.rfTotalPagesQty = rfTotalPagesQty;}

    private String rfFvi="";
    public String getRfFvi() {return rfFvi;}
    public void setRfFvi(String rfFvi) {this.rfFvi = rfFvi;}

    private String rfLvi="";
    public void setRfLvi(String rfLvi) {this.rfLvi = rfLvi;}
    public String getRfLvi() {return rfLvi;}

    private String ptsBusinessUnitDesc="";
    public String getPtsBusinessUnitDesc() {return ptsBusinessUnitDesc;}
    public void setPtsBusinessUnitDesc(String ptsBusinessUnitDesc) {this.ptsBusinessUnitDesc = ptsBusinessUnitDesc;}

    private WorkExtendedPersons[] workExtendedPersons;
    public WorkExtendedPersons[] getWorkExtendedPersons() {return workExtendedPersons;}
    public void setWorkExtendedPersons(WorkExtendedPersons[] workExtendedPersons) {this.workExtendedPersons = workExtendedPersons;}

    private WorkExtendedEditorialBoard[] workExtendedEditorialBoard;
    public WorkExtendedEditorialBoard[] getWorkExtendedEditorialBoard() {return workExtendedEditorialBoard;}
    public void setWorkExtendedEditorialBoard(WorkExtendedEditorialBoard[] workExtendedEditorialBoard) {this.workExtendedEditorialBoard = workExtendedEditorialBoard;}

    private WorkExtendedSubjectAreas[] workExtendedSubjectAreas;
    public WorkExtendedSubjectAreas[] getWorkExtendedSubjectAreas() {return workExtendedSubjectAreas;}
    public void setWorkExtendedSubjectAreas(WorkExtendedSubjectAreas[] workExtendedSubjectAreas) {this.workExtendedSubjectAreas = workExtendedSubjectAreas;}

    private WorkExtendedUrls[] workExtendedUrls;
    public WorkExtendedUrls[] getWorkExtendedUrls() {return workExtendedUrls;}
    public void setWorkExtendedUrls(WorkExtendedUrls[] workExtendedUrls) {this.workExtendedUrls = workExtendedUrls;}

    private WorkExtendedMetrics[] workExtendedMetrics;
    public WorkExtendedMetrics[] getWorkExtendedMetrics() {return workExtendedMetrics;}
    public void setWorkExtendedMetrics(WorkExtendedMetrics[] workExtendedMetrics) {this.workExtendedMetrics = workExtendedMetrics;}

    public static class WorkExtendedEditorialBoard{//by Nishant @ 09 Feb 2021

    private HashMap<String, Object> extendedBoardRole;
    public void setExtendedBoardRole(HashMap<String, Object> extendedBoardRole) {this.extendedBoardRole = extendedBoardRole;}
    public HashMap<String, Object> getExtendedBoardRole() {return extendedBoardRole;}

    private HashMap<String,Object> extendedBoardMember;
    public HashMap<String, Object> getExtendedBoardMember() {return extendedBoardMember;}
    public void setExtendedBoardMember(HashMap<String, Object> extendedBoardMember) {this.extendedBoardMember = extendedBoardMember;}

    private String groupNumber;
    public String getGroupNumber() {return groupNumber;}
    public void setGroupNumber(String groupNumber) {this.groupNumber = groupNumber;}

    private String sequenceNumber;
    public String getSequenceNumber() {return sequenceNumber;}
    public void setSequenceNumber(String sequenceNumber) {this.sequenceNumber = sequenceNumber;}
}

    public static class WorkExtendedSubjectAreas{
        //by Nishant @ 09 Feb 2021
        private ExtendedSubjectArea extendedSubjectArea;
        public ExtendedSubjectArea getExtendedSubjectArea() {return extendedSubjectArea;}
        public void setExtendedSubjectArea(ExtendedSubjectArea extendedSubjectArea) {this.extendedSubjectArea = extendedSubjectArea;}
    }

    public static class WorkExtendedUrls{
        private ExtendedUrl extendedUrl;
        public ExtendedUrl getExtendedUrl() {return extendedUrl;}
        public void setExtendedUrl(ExtendedUrl extendedUrl) {this.extendedUrl = extendedUrl;}
    }

    public static class WorkExtendedMetrics{
        private ExtendedMetric extendedMetric;
        public ExtendedMetric getExtendedMetric() {return extendedMetric;}
        public void setExtendedMetric(ExtendedMetric extendedMetric) {this.extendedMetric = extendedMetric;}
    }

    public void compareWithDB()
    {
        Log.info("----- verifiying workExtended data...");
        //Assert.assertEquals(primarySiteSystem, DataQualityContext.workExtendedTestClass.getWorkExtended().getPrimarySiteSystem());    printLog("primarySiteSystem");
     //   Assert.assertEquals(primarySiteAcronym, DataQualityContext.workExtendedTestClass.getWorkExtended().getPrimarySiteAcronym());        printLog("primarySiteAcronym");
    //    Assert.assertEquals(primarySiteSupportLevel, DataQualityContext.workExtendedTestClass.getWorkExtended().getPrimarySiteSupportLevel());        printLog("primarySiteSupportLevel");
    //    Assert.assertEquals(issueProdTypeCode, DataQualityContext.workExtendedTestClass.getWorkExtended().getIssueProdTypeCode());        printLog("issueProdTypeCode");
        //Assert.assertEquals(catalogueVolumesQty, DataQualityContext.workExtendedTestClass.getWorkExtended().getCatalogueVolumesQty());        printLog("catalogueVolumesQty");


        Assert.assertEquals(imageFileRef, DataQualityContext.workExtendedTestClass.getWorkExtended().getImageFileRef());
        printLog("imageFileRef");

        Assert.assertEquals(masterISBN, DataQualityContext.workExtendedTestClass.getWorkExtended().getMasterISBN());
        printLog("masterISBN");

        Assert.assertEquals(authorByLineText, DataQualityContext.workExtendedTestClass.getWorkExtended().getAuthorByLineText());
        printLog("authorByLineText");

        Assert.assertEquals(internalElsDiv, DataQualityContext.workExtendedTestClass.getWorkExtended().getInternalElsDiv());
        printLog("internalElsDiv");

        Assert.assertEquals(profitCentre, DataQualityContext.workExtendedTestClass.getWorkExtended().getProfitCentre());
        printLog("profitCentre");

        Assert.assertEquals(textRefTrade, DataQualityContext.workExtendedTestClass.getWorkExtended().getTextRefTrade());
        printLog("textRefTrade");

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
        //workExtendedSubjectAreas - EPR-W-10B3N5
    }

    private void printLog(String verified){Log.info("verified..."+verified);}

}
