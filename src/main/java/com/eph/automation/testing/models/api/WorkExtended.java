package com.eph.automation.testing.models.api;

import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.StitchingExtended.WorkExtRelationshipsJson;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.eph.automation.testing.models.contexts.DataQualityContext.getBreadcrumbMessage;

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

    public void compareWithDB(String workId) {

        try {
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

            if (journalElsComInd == null) journalElsComInd = "";
            if (journalAimsScope == null) journalAimsScope = "";
            if (journalProdSiteCode == null) journalProdSiteCode = "";
            if (primarySiteSystem == null) primarySiteSystem = "";
            if (primarySiteAcronym == null) primarySiteAcronym = "";
            if (primarySiteSupportLevel == null) primarySiteSupportLevel = "";
            if (issueProdTypeCode == null) issueProdTypeCode = "";
            if (catalogueVolumesQty == null) catalogueVolumesQty = "";
            if(imageFileRef==null)imageFileRef="";

            List<Integer> ignore = new ArrayList<>();

            Assert.assertEquals(
                    getBreadcrumbMessage() + " - getJournalElsComInd",
                    journalElsComInd,
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getJournalElsComInd());
            printLog("getJournalElsComInd");

            Assert.assertEquals(
                    getBreadcrumbMessage() + " - getJournalAimsScope",
                    journalAimsScope,
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getJournalAimsScope());
            printLog("getJournalAimsScope");

            Assert.assertEquals(
                    getBreadcrumbMessage() + " - journalProdSiteCode",
                    journalProdSiteCode,
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getJournalProdSiteCode());
            printLog("journalProdSiteCode");

            Assert.assertEquals(
                    getBreadcrumbMessage() + " - primarySiteSystem",
                    primarySiteSystem,
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getPrimarySiteSystem());
            printLog("primarySiteSystem");

            Assert.assertEquals(
                    getBreadcrumbMessage() + " - primarySiteAcronym",
                    primarySiteAcronym,
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getPrimarySiteAcronym());
            printLog("primarySiteAcronym");

            Assert.assertEquals(
                    getBreadcrumbMessage() + " - primarySiteSupportLevel",
                    primarySiteSupportLevel,
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getPrimarySiteSupportLevel());
            printLog("primarySiteSupportLevel");

            Assert.assertEquals(
                    getBreadcrumbMessage() + " - issueProdTypeCode",
                    issueProdTypeCode,
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getIssueProdTypeCode());
            printLog("issueProdTypeCode");

            Assert.assertEquals(
                    getBreadcrumbMessage() + " - catalogueVolumesQty",
                    catalogueVolumesQty,
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getCatalogueVolumesQty());
            printLog("catalogueVolumesQty");

            Assert.assertEquals(
                    getBreadcrumbMessage() + " - imageFileRef",
                    imageFileRef,
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getImageFileRef());
            printLog("imageFileRef");

            Assert.assertEquals(
                    getBreadcrumbMessage() + " - masterISBN",
                    masterISBN,
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getMasterISBN());
            printLog("masterISBN");

            Assert.assertEquals(
                    getBreadcrumbMessage() + " - authorByLineText",
                    authorByLineText,
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getAuthorByLineText());
            printLog("authorByLineText");

            Assert.assertEquals(
                    getBreadcrumbMessage() + " - internalElsDiv",
                    internalElsDiv,
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getInternalElsDiv());
            printLog("internalElsDiv");

            Assert.assertEquals(
                    getBreadcrumbMessage() + " - profitCentre",
                    profitCentre,
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getProfitCentre());
            printLog("profitCentre");

            Assert.assertEquals(
                    getBreadcrumbMessage() + " - textRefTrade",
                    textRefTrade,
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getTextRefTrade());
            printLog("textRefTrade");

            Assert.assertEquals(
                    getBreadcrumbMessage() + " - catalogueIssuesQty",
                    catalogueIssuesQty,
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getCatalogueIssuesQty());
            printLog("catalogueIssuesQty");

            Assert.assertEquals(
                    getBreadcrumbMessage() + " - catalogueVolumeFrom",
                    catalogueVolumeFrom,
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getCatalogueVolumeFrom());
            printLog("catalogueVolumeFrom");

            Assert.assertEquals(
                    getBreadcrumbMessage() + " - catalogueVolumeTo",
                    catalogueVolumeTo,
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getCatalogueVolumeTo());
            printLog("catalogueVolumeTo");

            Assert.assertEquals(
                    getBreadcrumbMessage() + " - rfIssuesQty",
                    rfIssuesQty,
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getRfIssuesQty());
            printLog("rfIssuesQty");

            Assert.assertEquals(
                    getBreadcrumbMessage() + " - rfTotalPagesQty",
                    rfTotalPagesQty,
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getRfTotalPagesQty());
            printLog("rfTotalPagesQty");

            Assert.assertEquals(
                    getBreadcrumbMessage() + " - rfFvi",
                    rfFvi,
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getRfFvi());
            printLog("rfFvi");

            Assert.assertEquals(
                    getBreadcrumbMessage() + " - rfLvi",
                    rfLvi,
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getRfLvi());
            printLog("rfLvi");

            Assert.assertEquals(
                    getBreadcrumbMessage() + " - ptsBusinessUnitDesc",
                    ptsBusinessUnitDesc,
                    DataQualityContext.workExtendedTestClass.getWorkExtended().getPtsBusinessUnitDesc());
            printLog("ptsBusinessUnitDesc");

            if (workExtendedPersons != null) {
                WorkExtendedPersons[] wep = workExtendedPersons.clone();
                WorkExtendedPersons[] wepDB =
                        DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedPersons.clone();

                Assert.assertEquals(
                        getBreadcrumbMessage() + " - workExtendedPersons-> count",
                        wep.length,
                        wepDB.length);
                printLog("workExtendedPersons count " + wep.length);
                ignore.clear();
                for (int i = 0; i < wepDB.length; i++) {
                    boolean extPersonFound = false;
                    for (int cnt = 0; cnt < wep.length; cnt++) {
                        if (ignore.contains(cnt)) continue;

                        if (wep[cnt].getCoreWorkPersonRoleId() != null
                                | wepDB[cnt].getCoreWorkPersonRoleId() != null) {
                            if (wep[cnt]
                                    .getCoreWorkPersonRoleId()
                                    .equalsIgnoreCase(wepDB[cnt].getCoreWorkPersonRoleId())) extPersonFound = true;
                        } else if (wep[cnt]
                                .getExtendedRole()
                                .get("code")
                                .toString()
                                .equalsIgnoreCase(wepDB[i].getExtendedRole().get("code").toString())
                                & wep[cnt]
                                .getExtendedPerson()
                                .get("firstName")
                                .toString()
                                .equalsIgnoreCase(wepDB[i].getExtendedPerson().get("firstName").toString())) {
                            extPersonFound = true;
                        }

                        if (extPersonFound) {
                            //   Assert.assertEquals(getBreadcrumbMessage() + " - extendedRole
                            // name", wep[cnt].getExtendedRole().get("code"),
                            // wepDB[i].getExtendedRole().get("code"));
                            printLog("extendedRole code" + i);
                            Assert.assertEquals(
                                    getBreadcrumbMessage() + " - extendedRole name",
                                    wep[cnt].getExtendedRole().get("name"),
                                    wepDB[i].getExtendedRole().get("name"));
                            printLog("extendedRole name");

                            //    Assert.assertEquals(getBreadcrumbMessage() + " - extendedPerson
                            // firstName", wep[cnt].getExtendedPerson().get("firstName"),
                            // wepDB[i].getExtendedPerson().get("firstName"));
                            printLog("extendedPerson firstName");
                            Assert.assertEquals(
                                    getBreadcrumbMessage() + " - extendedPerson lastName",
                                    wep[cnt].getExtendedPerson().get("lastName"),
                                    wepDB[i].getExtendedPerson().get("lastName"));
                            printLog("extendedPerson lastName");
                            Assert.assertEquals(
                                    getBreadcrumbMessage() + " - extendedPerson peoplehubId",
                                    wep[cnt].getExtendedPerson().get("peoplehubId"),
                                    wepDB[i].getExtendedPerson().get("peoplehubId"));
                            printLog("extendedPerson peoplehubId");
                            Assert.assertEquals(
                                    getBreadcrumbMessage() + " - extendedPerson email",
                                    wep[cnt].getExtendedPerson().get("email"),
                                    wepDB[i].getExtendedPerson().get("email"));
                            printLog("extendedPerson email");

                            // added by Nishant @ 8 Jun 2021
                            Assert.assertEquals(
                                    getBreadcrumbMessage() + " - extendedPerson lastName",
                                    wep[cnt].getExtendedPerson().get("notes"),
                                    wepDB[i].getExtendedPerson().get("notes"));
                            printLog("extendedPerson notes");
                            Assert.assertEquals(
                                    getBreadcrumbMessage() + " - extendedPerson lastName",
                                    wep[cnt].getExtendedPerson().get("affiliation"),
                                    wepDB[i].getExtendedPerson().get("affiliation"));
                            printLog("extendedPerson affiliation");
                            Assert.assertEquals(
                                    getBreadcrumbMessage() + " - extendedPerson lastName",
                                    wep[cnt].getExtendedPerson().get("imageUrl"),
                                    wepDB[i].getExtendedPerson().get("imageUrl"));
                            printLog("extendedPerson imageUrl");

                            Assert.assertEquals(
                                    getBreadcrumbMessage() + " - extendedPerson sequenceNumber",
                                    wep[cnt].getSequenceNumber(),
                                    wepDB[i].getSequenceNumber());
                            printLog("extendedPerson sequenceNumber");

                            ignore.add(cnt);
                            break;
                        }
                    }

                    Assert.assertTrue(
                            getBreadcrumbMessage() + "workExtendedPersons " + i + " found in DB",
                            extPersonFound);
                }
            }

            if (workExtendedEditorialBoard != null
                    | DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedEditorialBoard
                    != null) {
                // implemented by Nishant @ 24 May 2021, EPHD-3122
                // updated by Nishant @ 7 June 2021

                WorkExtendedEditorialBoard[] weeb = workExtendedEditorialBoard;
                WorkExtendedEditorialBoard[] weebDB =
                        DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedEditorialBoard;

                Assert.assertEquals(
                        getBreadcrumbMessage() + " - workExtendedEditorialBoard-> count",
                        weeb.length,
                        weebDB.length);
                printLog("workExtendedEditorialBoard count " + weeb.length);

                ignore.clear();
                boolean extendedBoardMemberMissing = false;
                boolean fNameMissing = false;
                boolean lNameMissing = false;
                boolean codeMatched = false;
                boolean firstNameMatched = false;
                boolean lastNameMatched = false;
                boolean exEditorialBoardFound = false;
                for (int i = 0; i < weeb.length; i++)
                {
                    exEditorialBoardFound = false;
                    codeMatched = false;
                    firstNameMatched = false;
                    lastNameMatched = false;
                    for (int cnt = 0; cnt <= weebDB.length; cnt++)
                    {
                        if (ignore.contains(cnt)) continue;

                         if (weeb[i].extendedBoardRole.get("code").toString().equalsIgnoreCase(weebDB[cnt].extendedBoardRole.get("code").toString()))
                         {
                            codeMatched = true;
                         }
                         else continue; //jump to next iteration if code not matching

                        if (weeb[i].extendedBoardMember != null & weebDB[cnt].extendedBoardMember != null)
                        {
              if (weeb[i].extendedBoardMember.get("firstName") != null
                  | weebDB[cnt].extendedBoardMember.get("firstName") != null) {
                if (weeb[i]
                    .extendedBoardMember
                    .get("firstName")
                    .toString()
                    .equalsIgnoreCase(weebDB[cnt].extendedBoardMember.get("firstName").toString()))
                  firstNameMatched = true;
}
              else fNameMissing=true;
              if (weeb[i].extendedBoardMember.get("lastName") != null
                  | weebDB[cnt].extendedBoardMember.get("lastName") != null) {
                if (weeb[i]
                    .extendedBoardMember
                    .get("lastName")
                    .toString()
                    .equalsIgnoreCase(weebDB[cnt].extendedBoardMember.get("lastName").toString()))
                  lastNameMatched = true;
}
              else lNameMissing=true;

              if(!fNameMissing &!lNameMissing)
              {if (codeMatched & firstNameMatched & lastNameMatched){exEditorialBoardFound = true;}}
              else if(!fNameMissing&lNameMissing)
              { if (codeMatched & firstNameMatched){exEditorialBoardFound = true;}}
              else if(!lNameMissing&fNameMissing)
              { if (codeMatched & lastNameMatched){exEditorialBoardFound = true;}}

                            if (exEditorialBoardFound)
                            {
                                printLog("workExtendedEditorialBoard extendedBoardMember firstName");
                                printLog("workExtendedEditorialBoard extendedBoardMember lastName");

                                Assert.assertEquals(
                                        getBreadcrumbMessage()
                                                + " - workExtendedEditorialBoard-> extendedBoardMember-> affiliation",
                                        weeb[i].extendedBoardMember.get("affiliation"),
                                        weebDB[cnt].extendedBoardMember.get("affiliation"));
                                printLog("workExtendedEditorialBoard extendedBoardMember affiliation");

                                if (weeb[i].extendedBoardMember.get("imageUrl") != null) {
                                  Assert.assertEquals(
                                      getBreadcrumbMessage()
                                          + " - workExtendedEditorialBoard-> extendedBoardMember-> imageUrl",
                                      weeb[i].extendedBoardMember.get("imageUrl"),
                                      weebDB[cnt].extendedBoardMember.get("imageUrl"));
                                  printLog("workExtendedEditorialBoard extendedBoardMember imageUrl");
                                    }
                            }
                            else continue;
                        }
else {extendedBoardMemberMissing = true;}
                          Assert.assertEquals(getBreadcrumbMessage()
                          + " - workExtendedEditorialBoard-> groupNumber",
                          weeb[i].groupNumber,weebDB[cnt].groupNumber);
                         printLog("workExtendedEditorialBoard groupNumber");

                         Assert.assertEquals(getBreadcrumbMessage()
                        + " - workExtendedEditorialBoard-> sequenceNumber",
                        weeb[i].sequenceNumber, weebDB[cnt].sequenceNumber);
                        printLog("workExtendedEditorialBoard sequenceNumber");

                        printLog("workExtendedEditorialBoard extendedBoardRole " + i);
                        ignore.add(cnt);
                        if(extendedBoardMemberMissing)exEditorialBoardFound=true;
                        break;

                    }

                    Assert.assertTrue(getBreadcrumbMessage()
                            + " - workExtendedEditorialBoard-> extendedBoardRole "
                            + i+ " not found in DB", exEditorialBoardFound);


                }
            }

            if (workExtendedSubjectAreas != null
                    | DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedSubjectAreas
                    != null) {
                // implemented by Nishant @ 24 May 2021, EPHD-3122
                // updated by Nishant @ 7 June 2021
                WorkExtendedSubjectAreas[] wesa = workExtendedSubjectAreas;
                WorkExtendedSubjectAreas[] wesaDB =
                        DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedSubjectAreas;

                Assert.assertEquals(
                        getBreadcrumbMessage() + " - workExtendedSubjectAreas-> count",
                        wesa.length,
                        wesaDB.length);
                printLog("workExtendedSubjectAreas count " + wesa.length);

                ignore.clear();
                for (int i = 0; i < wesa.length; i++) {
                    boolean exSubAreaFound = false;
                    for (int cnt = 0; cnt < wesaDB.length; cnt++) {
                        if (ignore.contains(cnt)) continue;

                        if (wesa[i]
                                .getExtendedSubjectArea()
                                .getCode()
                                .equalsIgnoreCase(wesaDB[cnt].getExtendedSubjectArea().getCode())) {
                            exSubAreaFound = true;
                            printLog("workExtendedSubjectAreas extendedSubjectArea code " + i);

                            Assert.assertEquals(
                                    getBreadcrumbMessage()
                                            + " - workExtendedSubjectAreas-> extendedSubjectArea-> name "
                                            + i,
                                    wesa[i].getExtendedSubjectArea().getName(),
                                    wesaDB[cnt].getExtendedSubjectArea().getName());
                            printLog("workExtendedSubjectAreas extendedSubjectArea name ");

                            Assert.assertEquals(
                                    getBreadcrumbMessage()
                                            + " - workExtendedSubjectAreas-> extendedSubjectArea-> Type-> code "
                                            + i,
                                    wesa[i].getExtendedSubjectArea().getType().get("code"),
                                    wesaDB[cnt].getExtendedSubjectArea().getType().get("code"));
                            printLog("workExtendedSubjectAreas extendedSubjectArea Type code ");

                            Assert.assertEquals(
                                    getBreadcrumbMessage()
                                            + " - workExtendedSubjectAreas-> extendedSubjectArea-> Type-> name "
                                            + i,
                                    wesa[i].getExtendedSubjectArea().getType().get("name"),
                                    wesaDB[cnt].getExtendedSubjectArea().getType().get("name"));
                            printLog("workExtendedSubjectAreas extendedSubjectArea Type name ");

                            Assert.assertEquals(
                                    getBreadcrumbMessage()
                                            + " - workExtendedSubjectAreas-> extendedSubjectArea-> priority "
                                            + i,
                                    wesa[i].getExtendedSubjectArea().getPriority(),
                                    wesaDB[cnt].getExtendedSubjectArea().getPriority());
                            printLog("workExtendedSubjectAreas extendedSubjectArea priority ");
                            ignore.add(cnt);
                            break;
                        }
                    }
                    Assert.assertTrue(
                            getBreadcrumbMessage()
                                    + " - workExtendedSubjectAreas-> extendedSubjectArea "
                                    + i
                                    + " found in DB ",
                            exSubAreaFound);
                }
            }

            if (workExtendedUrls != null
                    | DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedUrls != null) {
                // implemented by Nishant @ 25 May 2021 EPHD-3122

                WorkExtendedUrls[] weu = workExtendedUrls;
                WorkExtendedUrls[] weuDB =
                        DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedUrls;

                Assert.assertEquals(
                        getBreadcrumbMessage() + " - workExtendedUrls-> count",
                        weu.length,
                        weuDB.length);
                printLog("workExtendedUrls count " + weu.length);

                ignore.clear();
                for (int i = 0; i < weu.length; i++) {
                    boolean exUrlFound = false;
                    for (int cnt = 0; cnt < weuDB.length; cnt++) {
                        if (ignore.contains(cnt)) continue;
                        if (weu[i].extendedUrl.getUrl().equalsIgnoreCase(weuDB[cnt].extendedUrl.getUrl())) {
                            exUrlFound = true;

                            printLog("workExtendedUrls url ");

                            Assert.assertEquals(
                                    getBreadcrumbMessage() + " - workExtendedUrls-> code " + i,
                                    weu[i].extendedUrl.getType().get("code"),
                                    weuDB[cnt].extendedUrl.getType().get("code"));
                            printLog("workExtendedUrls code " + i);

                            Assert.assertEquals(
                                    getBreadcrumbMessage() + " - workExtendedUrls-> name " + i,
                                    weu[i].extendedUrl.getType().get("name"),
                                    weuDB[cnt].extendedUrl.getType().get("name"));
                            printLog("workExtendedUrls name ");

                            if (weu[i].extendedUrl.getUrlTitle() != null
                                    | weuDB[cnt].extendedUrl.getUrlTitle() != null) {
                                Assert.assertEquals(
                                        getBreadcrumbMessage() + " - workExtendedUrls-> urlTitle " + i,
                                        weu[i].extendedUrl.getUrlTitle(),
                                        weuDB[cnt].extendedUrl.getUrlTitle());
                                printLog("workExtendedUrls urlTitle ");
                            }
                            ignore.add(cnt);
                            break;
                        }
                    }
                    Assert.assertTrue(
                            getBreadcrumbMessage() + " - workExtendedUrls-> url " + i + " found",
                            exUrlFound);
                }
            }

            if (workExtendedMetrics != null
                    | DataQualityContext.workExtendedTestClass.getWorkExtended().getWorkExtendedMetrics()
                    != null) {
                // implemented by Nishant @ 25 May 2021 EPHD-3122

                WorkExtendedMetrics[] wem = workExtendedMetrics;
                WorkExtendedMetrics[] wemDB =
                        DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedMetrics;

                Assert.assertEquals(
                        getBreadcrumbMessage() + " - workExtendedMetrics-> count",
                        wem.length,
                        wemDB.length);
                printLog("workExtendedMetrics count " + wem.length);
                ignore.clear();
                for (int i = 0; i < wem.length; i++) {
                    boolean exMetricFound = false;
                    for (int cnt = 0; cnt < wemDB.length; cnt++) {
                        if (ignore.contains(cnt)) continue;
                        if (wem[i]
                                .getExtendedMetric()
                                .getType()
                                .get("code")
                                .toString()
                                .equalsIgnoreCase(
                                        wemDB[cnt].getExtendedMetric().getType().get("code").toString())) {
                            exMetricFound = true;
                            printLog("workExtendedMetrics code " + i);

                            Assert.assertEquals(
                                    getBreadcrumbMessage() + " - workExtendedMetrics-> name " + i,
                                    wem[i].getExtendedMetric().getType().get("name"),
                                    wemDB[cnt].getExtendedMetric().getType().get("name"));
                            printLog("workExtendedMetrics name ");

                            Assert.assertEquals(
                                    getBreadcrumbMessage() + " - workExtendedMetrics-> metric " + i,
                                    wem[i].getExtendedMetric().getMetric(),
                                    wemDB[cnt].getExtendedMetric().getMetric());
                            printLog("workExtendedMetrics metric ");

                            if (wem[i].getExtendedMetric().getMetricUrl() != null
                                    | wemDB[cnt].getExtendedMetric().getMetricUrl() != null) {
                                Assert.assertEquals(
                                        getBreadcrumbMessage() + " - workExtendedMetrics-> metric " + i,
                                        wem[i].getExtendedMetric().getMetricUrl(),
                                        wemDB[cnt].getExtendedMetric().getMetricUrl());
                                printLog("workExtendedMetrics metricUrl ");
                            }

                            if (wem[i].getExtendedMetric().getMetricYear() != null
                                    | wemDB[cnt].getExtendedMetric().getMetricYear() != null) {
                                Assert.assertEquals(
                                        getBreadcrumbMessage()
                                                + " - workExtendedMetrics-> metricYear"
                                                + i,
                                        wem[i].getExtendedMetric().getMetricYear(),
                                        wemDB[cnt].getExtendedMetric().getMetricYear());
                                printLog("workExtendedMetrics metricYear ");
                            }
                            ignore.add(cnt);
                            break;
                        }
                    }
                    Assert.assertTrue(
                            getBreadcrumbMessage() + " - workExtendedMetrics " + i + " found in DB",
                            exMetricFound);
                }
            }

            if (workExtendedRelationships != null
                    | DataQualityContext.workExtendedTestClass.getWorkExtended().workExtendedRelationships
                    != null) {
                // implemented by Nishant @ 25 May 2021 EPHD-3122
                WorkExtRelationshipsJson.ExtendedSibling[] wer =
                        workExtendedRelationships.getExtendedSibling();
                WorkExtRelationshipsJson.ExtendedSibling[] werDB =
                        DataQualityContext.workExtendedTestClass
                                .getWorkExtended()
                                .workExtendedRelationships
                                .getExtendedSibling();

                Assert.assertEquals(
                        getBreadcrumbMessage() + " - workExtRelationships-> count",
                        wer.length,
                        werDB.length);
                printLog("workExtRelationships count " + wer.length);

                for (int i = 0; i < wer.length; i++) {
                    Assert.assertEquals(
                            getBreadcrumbMessage() + " - workExtRelationships-> Typecode " + i,
                            wer[i].getType().get("code"),
                            werDB[i].getType().get("code"));
                    printLog("workExtRelationships Typecode " + i);

                    Assert.assertEquals(
                            getBreadcrumbMessage() + " - workExtRelationships-> Typecode " + i,
                            wer[i].getType().get("name"),
                            werDB[i].getType().get("name"));
                    printLog("workExtRelationships TypeName ");

                    Assert.assertEquals(
                            getBreadcrumbMessage() + " - workExtRelationships-> id " + i,
                            wer[i].getId(),
                            werDB[i].getId());
                    printLog("workExtRelationships id ");

                    Assert.assertEquals(
                            getBreadcrumbMessage()
                                    + " - workExtRelationships-> getWorkExtendedSummary->Title "
                                    + i,
                            wer[i].getWorkExtendedSummary().getTitle(),
                            werDB[i].getWorkExtendedSummary().getTitle());
                    printLog("workExtRelationships getWorkExtendedSummary title ");

                    Assert.assertEquals(
                            getBreadcrumbMessage()
                                    + " - workExtRelationships-> getWorkExtendedSummary->type code "
                                    + i,
                            wer[i].getWorkExtendedSummary().getType().get("code"),
                            werDB[i].getWorkExtendedSummary().getType().get("code"));
                    printLog("workExtRelationships getWorkExtendedSummary typeCode");

                    Assert.assertEquals(
                            getBreadcrumbMessage()
                                    + " - workExtRelationships-> getWorkExtendedSummary->type name "
                                    + i,
                            wer[i].getWorkExtendedSummary().getType().get("name"),
                            werDB[i].getWorkExtendedSummary().getType().get("name"));
                    printLog("workExtRelationships getWorkExtendedSummary typeName");

                    Assert.assertEquals(
                            getBreadcrumbMessage()
                                    + " - workExtRelationships-> getWorkExtendedSummary->typeRollUp "
                                    + i,
                            wer[i].getWorkExtendedSummary().getType().get("typeRollUp"),
                            werDB[i].getWorkExtendedSummary().getType().get("typeRollUp"));
                    printLog("workExtRelationships getWorkExtendedSummary typeRollUp");

                    Assert.assertEquals(
                            getBreadcrumbMessage()
                                    + " - workExtRelationships-> getWorkExtendedSummary->extendedStatus code"
                                    + i,
                            wer[i].getWorkExtendedSummary().getExtendedStatus().get("code"),
                            werDB[i].getWorkExtendedSummary().getExtendedStatus().get("code"));
                    printLog("workExtRelationships getWorkExtendedSummary extendedStatus code");

                    Assert.assertEquals(
                            getBreadcrumbMessage()
                                    + " - workExtRelationships-> getWorkExtendedSummary->extendedStatus name"
                                    + i,
                            wer[i].getWorkExtendedSummary().getExtendedStatus().get("name"),
                            werDB[i].getWorkExtendedSummary().getExtendedStatus().get("name"));
                    printLog("workExtRelationships getWorkExtendedSummary extendedStatus name");

                    Assert.assertEquals(
                            getBreadcrumbMessage()
                                    + " - workExtRelationships-> getWorkExtendedSummary->extendedStatus statusRollUp"
                                    + i,
                            wer[i].getWorkExtendedSummary().getExtendedStatus().get("statusRollUp"),
                            werDB[i].getWorkExtendedSummary().getExtendedStatus().get("statusRollUp"));
                    printLog("workExtRelationships getWorkExtendedSummary extendedStatus statusRollUp");
                }
            }

        } catch (Exception e) {
            Log.info(e.getCause().getMessage());
            Assert.assertFalse(
                    getBreadcrumbMessage()
                            + " e.message>"
                            + e.getMessage()
                            + " scenario Failed ",
                    true);
        }
    }
    private void printLog(String verified){Log.info("verified..."+verified);}

}
