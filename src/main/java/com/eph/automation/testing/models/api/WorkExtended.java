package com.eph.automation.testing.models.api;

import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.StitchingExtended.WorkExtRelationshipsJson;
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
    public String getJournalProdSiteCode() {if(journalProdSiteCode==null) return ""; else return journalProdSiteCode;}
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


    private WorkExtRelationshipsJson workExtRelationships;
    public WorkExtRelationshipsJson getWorkExtRelationships() {return workExtRelationships;}
    public void setWorkExtRelationships(WorkExtRelationshipsJson workExtRelationships) {this.workExtRelationships = workExtRelationships;}

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





    public void compareWithDB(String workId)
    {
        Log.info("----- verifiying workExtended data...");

        /*
        implemented by Nishant @ 24 May 2021, EPHD-3122
        * getJournalElsComInd
        * getJournalAimsScope
        * journalProdSiteCode
        * primarySiteSystem
        * primarySiteAcronym
        * primarySiteSupportLevel
        * issueProdTypeCode
        * catalogueVolumesQty
        * */

        if(journalElsComInd==null)journalElsComInd="";
        if(journalAimsScope==null)journalAimsScope="";
        if(journalProdSiteCode==null)journalProdSiteCode="";
        if(primarySiteSystem==null)primarySiteSystem="";
        if(primarySiteAcronym==null)primarySiteAcronym="";
        if(primarySiteSupportLevel==null)primarySiteSupportLevel="";
        if(issueProdTypeCode==null)issueProdTypeCode="";
        if(catalogueVolumesQty==null)catalogueVolumesQty="";


 //       if(!journalElsComInd.equalsIgnoreCase("")| !DataQualityContext.workExtendedTestClass.getWorkExtended().getJournalElsComInd().equalsIgnoreCase(""))
 //       {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - getJournalElsComInd", journalElsComInd, DataQualityContext.workExtendedTestClass.getWorkExtended().getJournalElsComInd());
            printLog("getJournalElsComInd");
 //       }

 //       if(journalAimsScope!=null | DataQualityContext.workExtendedTestClass.getWorkExtended().getJournalAimsScope()!=null)
 //       {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - getJournalAimsScope", journalAimsScope, DataQualityContext.workExtendedTestClass.getWorkExtended().getJournalAimsScope());
            printLog("getJournalAimsScope");
 //       }

 //       if(journalProdSiteCode!=null | DataQualityContext.workExtendedTestClass.getWorkExtended().getJournalProdSiteCode()!=null)
 //       {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - journalProdSiteCode", journalProdSiteCode, DataQualityContext.workExtendedTestClass.getWorkExtended().getJournalProdSiteCode());
            printLog("journalProdSiteCode");
  //      }

 //     if(primarySiteSystem!=null | DataQualityContext.workExtendedTestClass.getWorkExtended().getPrimarySiteSystem()!=null)
 //       {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - primarySiteSystem", primarySiteSystem, DataQualityContext.workExtendedTestClass.getWorkExtended().getPrimarySiteSystem());
            printLog("primarySiteSystem");
//        }

//        if(primarySiteAcronym!=null | DataQualityContext.workExtendedTestClass.getWorkExtended().getPrimarySiteAcronym()!=null) {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - primarySiteAcronym", primarySiteAcronym, DataQualityContext.workExtendedTestClass.getWorkExtended().getPrimarySiteAcronym());
            printLog("primarySiteAcronym");
 //       }

 //       if(primarySiteSupportLevel!=null| DataQualityContext.workExtendedTestClass.getWorkExtended().getPrimarySiteSupportLevel()!=null) {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - primarySiteSupportLevel", primarySiteSupportLevel, DataQualityContext.workExtendedTestClass.getWorkExtended().getPrimarySiteSupportLevel());
            printLog("primarySiteSupportLevel");
//        }

 //       if(issueProdTypeCode!=null| DataQualityContext.workExtendedTestClass.getWorkExtended().getIssueProdTypeCode()!=null) {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - issueProdTypeCode", issueProdTypeCode, DataQualityContext.workExtendedTestClass.getWorkExtended().getIssueProdTypeCode());
            printLog("issueProdTypeCode");
 //       }

 //       if(catalogueVolumesQty!=null| DataQualityContext.workExtendedTestClass.getWorkExtended().getCatalogueVolumesQty()!=null)
 //       {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - catalogueVolumesQty",catalogueVolumesQty, DataQualityContext.workExtendedTestClass.getWorkExtended().getCatalogueVolumesQty());
            printLog("catalogueVolumesQty");
 //       }

      Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - imageFileRef", imageFileRef, DataQualityContext.workExtendedTestClass.getWorkExtended().getImageFileRef());
        printLog("imageFileRef");

       Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - masterISBN", masterISBN, DataQualityContext.workExtendedTestClass.getWorkExtended().getMasterISBN());
        printLog("masterISBN");

       Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - authorByLineText", authorByLineText, DataQualityContext.workExtendedTestClass.getWorkExtended().getAuthorByLineText());
        printLog("authorByLineText");

       Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - internalElsDiv", internalElsDiv, DataQualityContext.workExtendedTestClass.getWorkExtended().getInternalElsDiv());
        printLog("internalElsDiv");

       Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - profitCentre", profitCentre, DataQualityContext.workExtendedTestClass.getWorkExtended().getProfitCentre());
        printLog("profitCentre");

       Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - textRefTrade", textRefTrade, DataQualityContext.workExtendedTestClass.getWorkExtended().getTextRefTrade());
        printLog("textRefTrade");

       Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - catalogueIssuesQty", catalogueIssuesQty, DataQualityContext.workExtendedTestClass.getWorkExtended().getCatalogueIssuesQty());
        printLog("catalogueIssuesQty");

       Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - catalogueVolumeFrom", catalogueVolumeFrom, DataQualityContext.workExtendedTestClass.getWorkExtended().getCatalogueVolumeFrom());
        printLog("catalogueVolumeFrom");

       Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - catalogueVolumeTo", catalogueVolumeTo, DataQualityContext.workExtendedTestClass.getWorkExtended().getCatalogueVolumeTo());
        printLog("catalogueVolumeTo");

       Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - rfIssuesQty", rfIssuesQty, DataQualityContext.workExtendedTestClass.getWorkExtended().getRfIssuesQty());
        printLog("rfIssuesQty");

       Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - rfTotalPagesQty", rfTotalPagesQty, DataQualityContext.workExtendedTestClass.getWorkExtended().getRfTotalPagesQty());
        printLog("rfTotalPagesQty");

       Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - rfFvi", rfFvi, DataQualityContext.workExtendedTestClass.getWorkExtended().getRfFvi());
        printLog("rfFvi");

       Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - rfLvi", rfLvi, DataQualityContext.workExtendedTestClass.getWorkExtended().getRfLvi());
        printLog("rfLvi");

       Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - ptsBusinessUnitDesc", ptsBusinessUnitDesc, DataQualityContext.workExtendedTestClass.getWorkExtended().getPtsBusinessUnitDesc());
        printLog("ptsBusinessUnitDesc");

       if(workExtendedPersons!=null) {
           WorkExtendedPersons[] wep = workExtendedPersons.clone();
           WorkExtendedPersons[] wepDB = DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedPersons.clone();

           Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedPersons-> count",wep.length, wepDB.length);
           printLog("workExtendedPersons count "+wep.length);

           for (int i = 0; i < workExtendedPersons.length; i++) {
              Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - extendedRole code"+i, wep[i].getExtendedRole().get("code"), wepDB[i].getExtendedRole().get("code"));
               printLog("extendedRole code"+i);
              Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - extendedRole name", wep[i].getExtendedRole().get("name"), wepDB[i].getExtendedRole().get("name"));
               printLog("extendedRole name");
              Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - extendedPerson firstName", wep[i].getExtendedPerson().get("firstName"), wepDB[i].getExtendedPerson().get("firstName"));
               printLog("extendedPerson firstName");
              Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - extendedPerson lastName", wep[i].getExtendedPerson().get("lastName"), wepDB[i].getExtendedPerson().get("lastName"));
               printLog("extendedPerson lastName");
              Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - extendedPerson peoplehubId", wep[i].getExtendedPerson().get("peoplehubId"), wepDB[i].getExtendedPerson().get("peoplehubId"));
               printLog("extendedPerson peoplehubId");
              Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - extendedPerson email", wep[i].getExtendedPerson().get("email"), wepDB[i].getExtendedPerson().get("email"));
               printLog("extendedPerson email");
           }
       }

       if(workExtendedEditorialBoard!=null |DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedEditorialBoard!=null){
           //implemented by Nishant @ 24 May 2021, EPHD-3122
           WorkExtendedEditorialBoard[] weeb = workExtendedEditorialBoard;
           WorkExtendedEditorialBoard[] weebDB = DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedEditorialBoard;

           Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedEditorialBoard-> count",weeb.length, weebDB.length);
           printLog("workExtendedEditorialBoard count " +weeb.length);

           for(int i=0;i<weeb.length;i++)
           {
               Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedEditorialBoard-> extendedBoardRole "+i,weeb[i].extendedBoardRole.get("code"), weebDB[i].extendedBoardRole.get("code"));
               printLog("workExtendedEditorialBoard extendedBoardRole "+i);

               Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedEditorialBoard-> extendedBoardMember-> firstName",weeb[i].extendedBoardMember.get("firstName"), weebDB[i].extendedBoardMember.get("firstName"));
               printLog("workExtendedEditorialBoard extendedBoardMember firstName");

               Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedEditorialBoard-> extendedBoardMember-> lastName",weeb[i].extendedBoardMember.get("lastName"), weebDB[i].extendedBoardMember.get("lastName"));
               printLog("workExtendedEditorialBoard extendedBoardMember lastName");

               Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedEditorialBoard-> extendedBoardMember-> affiliation",weeb[i].extendedBoardMember.get("affiliation"), weebDB[i].extendedBoardMember.get("affiliation"));
               printLog("workExtendedEditorialBoard extendedBoardMember affiliation");

               Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedEditorialBoard-> groupNumber",weeb[i].groupNumber, weebDB[i].groupNumber);
               printLog("workExtendedEditorialBoard groupNumber");

               Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedEditorialBoard-> sequenceNumber",weeb[i].sequenceNumber, weebDB[i].sequenceNumber);
               printLog("workExtendedEditorialBoard sequenceNumber");
           }
       }

        if(workExtendedSubjectAreas!=null |DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedSubjectAreas!=null){
            //implemented by Nishant @ 24 May 2021, EPHD-3122
            WorkExtendedSubjectAreas[] wesa = workExtendedSubjectAreas;
            WorkExtendedSubjectAreas[] wesaDB = DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedSubjectAreas;

            Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedSubjectAreas-> count",wesa.length, wesaDB.length);
            printLog("workExtendedSubjectAreas count " +wesa.length);

            for(int i=0;i<wesa.length;i++)
            {
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedSubjectAreas-> extendedSubjectArea-> code "+i,wesa[i].getExtendedSubjectArea().getCode(), wesaDB[i].getExtendedSubjectArea().getCode());
                printLog("workExtendedSubjectAreas extendedSubjectArea code "+i);

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedSubjectAreas-> extendedSubjectArea-> name "+i,wesa[i].getExtendedSubjectArea().getName(), wesaDB[i].getExtendedSubjectArea().getName());
                printLog("workExtendedSubjectAreas extendedSubjectArea name ");

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedSubjectAreas-> extendedSubjectArea-> Type-> code "+i,wesa[i].getExtendedSubjectArea().getType().get("code"), wesaDB[i].getExtendedSubjectArea().getType().get("code"));
                printLog("workExtendedSubjectAreas extendedSubjectArea Type code ");

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedSubjectAreas-> extendedSubjectArea-> Type-> name "+i,wesa[i].getExtendedSubjectArea().getType().get("name"), wesaDB[i].getExtendedSubjectArea().getType().get("name"));
                printLog("workExtendedSubjectAreas extendedSubjectArea Type name ");

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedSubjectAreas-> extendedSubjectArea-> priority "+i,wesa[i].getExtendedSubjectArea().getPriority(), wesaDB[i].getExtendedSubjectArea().getPriority());
                printLog("workExtendedSubjectAreas extendedSubjectArea priority ");
            }
        }

        if(workExtendedUrls!=null|DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedUrls!=null){
            //implemented by Nishant @ 25 May 2021 EPHD-3122

            WorkExtendedUrls[] weu = workExtendedUrls;
            WorkExtendedUrls[] weuDB = DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedUrls;

            Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedUrls-> count",weu.length, weuDB.length);
            printLog("workExtendedUrls count " +weu.length);

            for(int i=0;i<weu.length;i++)
            {
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedUrls-> code "+i,weu[i].extendedUrl.getType().get("code"), weuDB[i].extendedUrl.getType().get("code"));
                printLog("workExtendedUrls code "+i);

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedUrls-> name "+i,weu[i].extendedUrl.getType().get("name"), weuDB[i].extendedUrl.getType().get("name"));
                printLog("workExtendedUrls name ");

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedUrls-> url "+i,weu[i].extendedUrl.getUrl(), weuDB[i].extendedUrl.getUrl());
                printLog("workExtendedUrls url ");

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedUrls-> urlTitle "+i,weu[i].extendedUrl.getUrlTitle(), weuDB[i].extendedUrl.getUrlTitle());
                printLog("workExtendedUrls urlTitle ");

            }
        }

        if(workExtendedMetrics!=null|DataQualityContext.workExtendedTestClass.getWorkExtended().getWorkExtendedMetrics()!=null) {
            //implemented by Nishant @ 25 May 2021 EPHD-3122

            WorkExtendedMetrics[] wem = workExtendedMetrics;
            WorkExtendedMetrics[] wemDB = DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedMetrics;

            Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedMetrics-> count", wem.length, wemDB.length);
            printLog("workExtendedMetrics count " + wem.length);

            for (int i = 0; i < wem.length; i++) {
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedMetrics-> code " + i, wem[i].getExtendedMetric().getType().get("code"), wemDB[i].getExtendedMetric().getType().get("code"));
                printLog("workExtendedMetrics code " + i);

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedMetrics-> name " + i, wem[i].getExtendedMetric().getType().get("name"), wemDB[i].getExtendedMetric().getType().get("name"));
                printLog("workExtendedMetrics name ");

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedMetrics-> metric " + i, wem[i].getExtendedMetric().getMetric(), wemDB[i].getExtendedMetric().getMetric());
                printLog("workExtendedMetrics metric ");

                if(wem[i].getExtendedMetric().getMetricUrl()!=null|wemDB[i].getExtendedMetric().getMetricUrl()!=null) {
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedMetrics-> metric " + i, wem[i].getExtendedMetric().getMetricUrl(), wemDB[i].getExtendedMetric().getMetricUrl());
                    printLog("workExtendedMetrics metricUrl ");
                }

                if(wem[i].getExtendedMetric().getMetricYear()!=null|wemDB[i].getExtendedMetric().getMetricYear()!=null) {
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedMetrics-> metricYear" + i, wem[i].getExtendedMetric().getMetricYear(), wemDB[i].getExtendedMetric().getMetricYear());
                    printLog("workExtendedMetrics metricYear ");
                }
            }
        }



    }

    private void printLog(String verified){Log.info("verified..."+verified);}

}
