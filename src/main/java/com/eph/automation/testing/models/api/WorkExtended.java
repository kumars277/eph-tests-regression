package com.eph.automation.testing.models.api;

import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.StitchingExtended.WorkExtRelationshipsJson;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * created by Nishant @ 19 Jun 2020
 * for jrbi data reflect in APIv3 work extended, person extended and manifestation extended
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


    private WorkExtRelationshipsJson workExtendedRelationships;
    public WorkExtRelationshipsJson getWorkExtendedRelationships() {return workExtendedRelationships;   }
    public void setWorkExtendedRelationships(WorkExtRelationshipsJson workExtendedRelationships) {this.workExtendedRelationships = workExtendedRelationships;}

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

        List<Integer> ignore = new ArrayList<>();

    Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - getJournalElsComInd", journalElsComInd, DataQualityContext.workExtendedTestClass.getWorkExtended().getJournalElsComInd());
    printLog("getJournalElsComInd");

    Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - getJournalAimsScope", journalAimsScope, DataQualityContext.workExtendedTestClass.getWorkExtended().getJournalAimsScope());
    printLog("getJournalAimsScope");

    Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - journalProdSiteCode", journalProdSiteCode, DataQualityContext.workExtendedTestClass.getWorkExtended().getJournalProdSiteCode());
    printLog("journalProdSiteCode");

    Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - primarySiteSystem", primarySiteSystem, DataQualityContext.workExtendedTestClass.getWorkExtended().getPrimarySiteSystem());
    printLog("primarySiteSystem");

    Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - primarySiteAcronym", primarySiteAcronym, DataQualityContext.workExtendedTestClass.getWorkExtended().getPrimarySiteAcronym());
    printLog("primarySiteAcronym");

    Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - primarySiteSupportLevel", primarySiteSupportLevel, DataQualityContext.workExtendedTestClass.getWorkExtended().getPrimarySiteSupportLevel());
    printLog("primarySiteSupportLevel");

    Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - issueProdTypeCode", issueProdTypeCode, DataQualityContext.workExtendedTestClass.getWorkExtended().getIssueProdTypeCode());
    printLog("issueProdTypeCode");

    Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - catalogueVolumesQty",catalogueVolumesQty, DataQualityContext.workExtendedTestClass.getWorkExtended().getCatalogueVolumesQty());
    printLog("catalogueVolumesQty");

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

        if (workExtendedPersons != null) {
            WorkExtendedPersons[] wep = workExtendedPersons.clone();
            WorkExtendedPersons[] wepDB = DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedPersons.clone();

            Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedPersons-> count", wep.length, wepDB.length);
            printLog("workExtendedPersons count " + wep.length);
            ignore.clear();
            for (int i = 0; i < wepDB.length; i++) {
                boolean extPersonFound = false;
                for (int cnt = 0; cnt < wep.length; cnt++) {
                    if (ignore.contains(cnt)) continue;

                    if (wep[cnt].getCoreWorkPersonRoleId() != null | wepDB[cnt].getCoreWorkPersonRoleId() != null) {
                        if (wep[cnt].getCoreWorkPersonRoleId().equalsIgnoreCase(wepDB[cnt].getCoreWorkPersonRoleId()))
                            extPersonFound = true;
                    } else if (wep[cnt].getExtendedRole().get("code").toString().equalsIgnoreCase(wepDB[i].getExtendedRole().get("code").toString()) &
                            wep[cnt].getExtendedPerson().get("firstName").toString().equalsIgnoreCase(wepDB[i].getExtendedPerson().get("firstName").toString())
                    ) {
                        extPersonFound = true;
                    }

                    if(extPersonFound){
                     //   Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - extendedRole name", wep[cnt].getExtendedRole().get("code"), wepDB[i].getExtendedRole().get("code"));
                        printLog("extendedRole code" + i);
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - extendedRole name", wep[cnt].getExtendedRole().get("name"), wepDB[i].getExtendedRole().get("name"));
                        printLog("extendedRole name");

                    //    Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - extendedPerson firstName", wep[cnt].getExtendedPerson().get("firstName"), wepDB[i].getExtendedPerson().get("firstName"));
                        printLog("extendedPerson firstName");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - extendedPerson lastName", wep[cnt].getExtendedPerson().get("lastName"), wepDB[i].getExtendedPerson().get("lastName"));
                        printLog("extendedPerson lastName");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - extendedPerson peoplehubId", wep[cnt].getExtendedPerson().get("peoplehubId"), wepDB[i].getExtendedPerson().get("peoplehubId"));
                        printLog("extendedPerson peoplehubId");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - extendedPerson email", wep[cnt].getExtendedPerson().get("email"), wepDB[i].getExtendedPerson().get("email"));
                        printLog("extendedPerson email");

                        //added by Nishant @ 8 Jun 2021
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - extendedPerson lastName", wep[cnt].getExtendedPerson().get("notes"), wepDB[i].getExtendedPerson().get("notes"));
                        printLog("extendedPerson notes");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - extendedPerson lastName", wep[cnt].getExtendedPerson().get("affiliation"), wepDB[i].getExtendedPerson().get("affiliation"));
                        printLog("extendedPerson affiliation");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - extendedPerson lastName", wep[cnt].getExtendedPerson().get("imageUrl"), wepDB[i].getExtendedPerson().get("imageUrl"));
                        printLog("extendedPerson imageUrl");

                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - extendedPerson sequenceNumber", wep[cnt].getSequenceNumber(), wepDB[i].getSequenceNumber());
                        printLog("extendedPerson sequenceNumber");


                        ignore.add(cnt);
                        break;
                    }
                }

                Assert.assertTrue(DataQualityContext.breadcrumbMessage + "workExtendedPersons " + i + " found in DB", extPersonFound);
            }
        }

    if(workExtendedEditorialBoard!=null |DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedEditorialBoard!=null){
       //implemented by Nishant @ 24 May 2021, EPHD-3122
     //updated by Nishant @ 7 June 2021

       WorkExtendedEditorialBoard[] weeb = workExtendedEditorialBoard;
       WorkExtendedEditorialBoard[] weebDB = DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedEditorialBoard;

       Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedEditorialBoard-> count",weeb.length, weebDB.length);
       printLog("workExtendedEditorialBoard count " +weeb.length);

       ignore.clear();
       boolean codeMatched = false;
       boolean firstNameMatched = false;
       boolean lastNameMatched =false;

       for(int i=0;i<weeb.length;i++)
       {
        boolean exEditorialBoardFound = false;
           codeMatched = false;firstNameMatched = false; lastNameMatched =false;
        for(int cnt=0;cnt<=weebDB.length;cnt++){
            if (ignore.contains(cnt)) continue;
            if(weeb[i].extendedBoardMember==null&weebDB[cnt].extendedBoardMember==null)continue;
            else
            {
                if (weeb[i].extendedBoardMember.get("firstName") != null | weebDB[cnt].extendedBoardMember.get("firstName") != null)
                    if (weeb[i].extendedBoardMember.get("firstName").toString().equalsIgnoreCase(weebDB[cnt].extendedBoardMember.get("firstName").toString()))
                        firstNameMatched = true;
                if (weeb[i].extendedBoardMember.get("lastName") != null | weebDB[cnt].extendedBoardMember.get("lastName") != null)
                    if (weeb[i].extendedBoardMember.get("lastName").toString().equalsIgnoreCase(weebDB[cnt].extendedBoardMember.get("lastName").toString()))
                        lastNameMatched = true;
            }
                if(weeb[i].extendedBoardRole.get("code").toString().equalsIgnoreCase(weebDB[cnt].extendedBoardRole.get("code").toString()))codeMatched=true;

            if((codeMatched&firstNameMatched)|(codeMatched&lastNameMatched)) {
                exEditorialBoardFound = true;

                printLog("workExtendedEditorialBoard extendedBoardRole " + i);
                printLog("workExtendedEditorialBoard extendedBoardMember firstName");
                printLog("workExtendedEditorialBoard extendedBoardMember lastName");

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedEditorialBoard-> extendedBoardMember-> affiliation", weeb[i].extendedBoardMember.get("affiliation"), weebDB[cnt].extendedBoardMember.get("affiliation"));
                printLog("workExtendedEditorialBoard extendedBoardMember affiliation");

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedEditorialBoard-> groupNumber", weeb[i].groupNumber, weebDB[cnt].groupNumber);
                printLog("workExtendedEditorialBoard groupNumber");

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedEditorialBoard-> sequenceNumber", weeb[i].sequenceNumber, weebDB[cnt].sequenceNumber);
                printLog("workExtendedEditorialBoard sequenceNumber");

                ignore.add(cnt);
                break;
            }
        }

        Assert.assertTrue(DataQualityContext.breadcrumbMessage + " - workExtendedEditorialBoard-> extendedBoardRole "+i+" not found in DB",exEditorialBoardFound);


       }
   }

    if(workExtendedSubjectAreas!=null |DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedSubjectAreas!=null){
        //implemented by Nishant @ 24 May 2021, EPHD-3122
        //updated by Nishant @ 7 June 2021
        WorkExtendedSubjectAreas[] wesa = workExtendedSubjectAreas;
        WorkExtendedSubjectAreas[] wesaDB = DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedSubjectAreas;

        Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedSubjectAreas-> count",wesa.length, wesaDB.length);
        printLog("workExtendedSubjectAreas count " +wesa.length);

        ignore.clear();
        for(int i=0;i<wesa.length;i++)
        {
            boolean exSubAreaFound = false;
            for(int cnt=0;cnt<wesaDB.length;cnt++) {
                if(ignore.contains(cnt)) continue;

                if (wesa[i].getExtendedSubjectArea().getCode().equalsIgnoreCase(wesaDB[cnt].getExtendedSubjectArea().getCode())) {
                    exSubAreaFound = true;
                    printLog("workExtendedSubjectAreas extendedSubjectArea code " + i);

                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedSubjectAreas-> extendedSubjectArea-> name " + i, wesa[i].getExtendedSubjectArea().getName(), wesaDB[cnt].getExtendedSubjectArea().getName());
                    printLog("workExtendedSubjectAreas extendedSubjectArea name ");

                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedSubjectAreas-> extendedSubjectArea-> Type-> code " + i, wesa[i].getExtendedSubjectArea().getType().get("code"), wesaDB[cnt].getExtendedSubjectArea().getType().get("code"));
                    printLog("workExtendedSubjectAreas extendedSubjectArea Type code ");

                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedSubjectAreas-> extendedSubjectArea-> Type-> name " + i, wesa[i].getExtendedSubjectArea().getType().get("name"), wesaDB[cnt].getExtendedSubjectArea().getType().get("name"));
                    printLog("workExtendedSubjectAreas extendedSubjectArea Type name ");

                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedSubjectAreas-> extendedSubjectArea-> priority " + i, wesa[i].getExtendedSubjectArea().getPriority(), wesaDB[cnt].getExtendedSubjectArea().getPriority());
                    printLog("workExtendedSubjectAreas extendedSubjectArea priority ");
                ignore.add(cnt);break;
                }
            }
            Assert.assertTrue(DataQualityContext.breadcrumbMessage + " - workExtendedSubjectAreas-> extendedSubjectArea "+i+" found in DB ",exSubAreaFound);
        }
    }

    if(workExtendedUrls!=null|DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedUrls!=null){
        //implemented by Nishant @ 25 May 2021 EPHD-3122

        WorkExtendedUrls[] weu = workExtendedUrls;
        WorkExtendedUrls[] weuDB = DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedUrls;

        Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedUrls-> count",weu.length, weuDB.length);
        printLog("workExtendedUrls count " +weu.length);

        ignore.clear();
        for(int i=0;i<weu.length;i++)
        {
            boolean exUrlFound = false;
            for(int cnt=0;cnt<weuDB.length;cnt++) {
                if(ignore.contains(cnt))continue;
                if (weu[i].extendedUrl.getUrl().equalsIgnoreCase(weuDB[cnt].extendedUrl.getUrl())) {
                    exUrlFound=true;

                    printLog("workExtendedUrls url ");

                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedUrls-> code " + i, weu[i].extendedUrl.getType().get("code"), weuDB[cnt].extendedUrl.getType().get("code"));
                    printLog("workExtendedUrls code " + i);

                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedUrls-> name " + i, weu[i].extendedUrl.getType().get("name"), weuDB[cnt].extendedUrl.getType().get("name"));
                    printLog("workExtendedUrls name ");

                    if(weu[i].extendedUrl.getUrlTitle()!=null|weuDB[cnt].extendedUrl.getUrlTitle()!=null) {
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedUrls-> urlTitle " + i, weu[i].extendedUrl.getUrlTitle(), weuDB[cnt].extendedUrl.getUrlTitle());
                        printLog("workExtendedUrls urlTitle ");
                    }
                    ignore.add(cnt);break;
                }
            }
            Assert.assertTrue(DataQualityContext.breadcrumbMessage + " - workExtendedUrls-> url "+i+" found",exUrlFound);
        }
    }

        if (workExtendedMetrics != null | DataQualityContext.workExtendedTestClass.getWorkExtended().getWorkExtendedMetrics() != null) {
            //implemented by Nishant @ 25 May 2021 EPHD-3122

            WorkExtendedMetrics[] wem = workExtendedMetrics;
            WorkExtendedMetrics[] wemDB = DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedMetrics;

            Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedMetrics-> count", wem.length, wemDB.length);
            printLog("workExtendedMetrics count " + wem.length);
            ignore.clear();
            for (int i = 0; i < wem.length; i++) {
                boolean exMetricFound = false;
                for (int cnt = 0; cnt < wemDB.length; cnt++) {
                    if (ignore.contains(cnt)) continue;
                    if (wem[i].getExtendedMetric().getType().get("code").toString().equalsIgnoreCase(wemDB[cnt].getExtendedMetric().getType().get("code").toString())) {
                        exMetricFound = true;
                        printLog("workExtendedMetrics code " + i);

                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedMetrics-> name " + i, wem[i].getExtendedMetric().getType().get("name"), wemDB[cnt].getExtendedMetric().getType().get("name"));
                        printLog("workExtendedMetrics name ");

                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedMetrics-> metric " + i, wem[i].getExtendedMetric().getMetric(), wemDB[cnt].getExtendedMetric().getMetric());
                        printLog("workExtendedMetrics metric ");

                        if (wem[i].getExtendedMetric().getMetricUrl() != null | wemDB[cnt].getExtendedMetric().getMetricUrl() != null) {
                            Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedMetrics-> metric " + i, wem[i].getExtendedMetric().getMetricUrl(), wemDB[cnt].getExtendedMetric().getMetricUrl());
                            printLog("workExtendedMetrics metricUrl ");
                        }

                        if (wem[i].getExtendedMetric().getMetricYear() != null | wemDB[cnt].getExtendedMetric().getMetricYear() != null) {
                            Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtendedMetrics-> metricYear" + i, wem[i].getExtendedMetric().getMetricYear(), wemDB[cnt].getExtendedMetric().getMetricYear());
                            printLog("workExtendedMetrics metricYear ");
                        }
                        ignore.add(cnt);
                        break;
                    }
                }
                Assert.assertTrue(DataQualityContext.breadcrumbMessage + " - workExtendedMetrics " + i + " found in DB", exMetricFound);
            }
        }

    if(workExtendedRelationships!=null|DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedRelationships!=null){
            //implemented by Nishant @ 25 May 2021 EPHD-3122
            WorkExtRelationshipsJson.ExtendedSibling[] wer = workExtendedRelationships.getExtendedSibling();
            WorkExtRelationshipsJson.ExtendedSibling[] werDB = DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedRelationships.getExtendedSibling();

            Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtRelationships-> count",wer.length, werDB.length);
            printLog("workExtRelationships count " +wer.length);

            for(int i=0;i<wer.length;i++)
            {
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtRelationships-> Typecode "+i,wer[i].getType().get("code"), werDB[i].getType().get("code"));
                printLog("workExtRelationships Typecode "+i);

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtRelationships-> Typecode "+i,wer[i].getType().get("name"), werDB[i].getType().get("name"));
                printLog("workExtRelationships TypeName ");


                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtRelationships-> id "+i,wer[i].getId(), werDB[i].getId());
                printLog("workExtRelationships id ");

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtRelationships-> getWorkExtendedSummary->Title "+i,wer[i].getWorkExtendedSummary().getTitle(), werDB[i].getWorkExtendedSummary().getTitle());
                printLog("workExtRelationships getWorkExtendedSummary title ");


                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtRelationships-> getWorkExtendedSummary->type code "+i,
                        wer[i].getWorkExtendedSummary().getType().get("code"), werDB[i].getWorkExtendedSummary().getType().get("code"));
                printLog("workExtRelationships getWorkExtendedSummary typeCode");


                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtRelationships-> getWorkExtendedSummary->type name "+i,
                        wer[i].getWorkExtendedSummary().getType().get("name"), werDB[i].getWorkExtendedSummary().getType().get("name"));
                printLog("workExtRelationships getWorkExtendedSummary typeName");


                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtRelationships-> getWorkExtendedSummary->typeRollUp "+i,
                        wer[i].getWorkExtendedSummary().getType().get("typeRollUp"), werDB[i].getWorkExtendedSummary().getType().get("typeRollUp"));
                printLog("workExtRelationships getWorkExtendedSummary typeRollUp");

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtRelationships-> getWorkExtendedSummary->extendedStatus code"+i,
                        wer[i].getWorkExtendedSummary().getExtendedStatus().get("code"), werDB[i].getWorkExtendedSummary().getExtendedStatus().get("code"));
                printLog("workExtRelationships getWorkExtendedSummary extendedStatus code");

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtRelationships-> getWorkExtendedSummary->extendedStatus name"+i,
                        wer[i].getWorkExtendedSummary().getExtendedStatus().get("name"), werDB[i].getWorkExtendedSummary().getExtendedStatus().get("name"));
                printLog("workExtRelationships getWorkExtendedSummary extendedStatus name");

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - workExtRelationships-> getWorkExtendedSummary->extendedStatus statusRollUp"+i,
                        wer[i].getWorkExtendedSummary().getExtendedStatus().get("statusRollUp"), werDB[i].getWorkExtendedSummary().getExtendedStatus().get("statusRollUp"));
                printLog("workExtRelationships getWorkExtendedSummary extendedStatus statusRollUp");
            }
        }


    }

    private void printLog(String verified){Log.info("verified..."+verified);}

}
