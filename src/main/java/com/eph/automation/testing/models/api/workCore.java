package com.eph.automation.testing.models.api;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.AccountableProductDataObject;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;
import net.minidev.json.parser.ParseException;
import org.junit.Assert;

import java.util.*;

/*
* created by Nishant @ 8 May 2020
* updated by Nishant @ 29 Jan 2021, EPHD-2747
* */
@JsonIgnoreProperties(ignoreUnknown = true)
public class workCore {

    private List<WorkDataObject> workDataObjectsFromEPHGD;
    public List<WorkDataObject> getWorkDataObjectsFromEPHGD() {return workDataObjectsFromEPHGD;}
    public void setWorkDataObjectsFromEPHGD(List<WorkDataObject> workDataObjectsFromEPHGD) {this.workDataObjectsFromEPHGD = workDataObjectsFromEPHGD;}

    private List<AccountableProductDataObject> accountableProductDataObjectsFromEPHGD;
    public List<AccountableProductDataObject> getAccountableProductDataObjectsFromEPHGD() {return accountableProductDataObjectsFromEPHGD;}
    public void setAccountableProductDataObjectsFromEPHGD(List<AccountableProductDataObject> accountableProductDataObjectsFromEPHGD) {this.accountableProductDataObjectsFromEPHGD = accountableProductDataObjectsFromEPHGD;}

    private String title;
    public String getTitle() {return title;}
    public void setTitle(String title) {this.title = title;}

    private String subTitle;
    public String getSubTitle() {return subTitle;}
    public void setSubTitle(String subTitle) {this.subTitle = subTitle;}

    private String electronicRightsInd;
    public String getElectronicRightsInd() {return electronicRightsInd;}
    public void setElectronicRightsInd(String electronicRightsInd) {this.electronicRightsInd = electronicRightsInd;}

    private HashMap<String, Object> language;
    public HashMap<String, Object> getLanguage() {return language;}
    public void setLanguage(HashMap<String, Object> language) {this.language = language;}

    private String editionNumber;
    public String getEditionNumber() {return editionNumber;}
    public void setEditionNumber(String editionNumber) {this.editionNumber = editionNumber;}

    private HashMap<String, Object> subscriptionType;
    public HashMap<String, Object> getSubscriptionType() {return subscriptionType;}
    public void setSubscriptionType(HashMap<String, Object> subscriptionType) {this.subscriptionType = subscriptionType;}

    private String volume;
    public String getVolume() {return volume;}
    public void setVolume(String volume) {this.volume = volume;}

    private String copyrightYear;
    public String getCopyrightYear() {return copyrightYear;}
    public void setCopyrightYear(String copyrightYear) {this.copyrightYear = copyrightYear;}

    private WorkIdentifiersApiObject[] identifiers;
    public WorkIdentifiersApiObject[] getIdentifiers() {return identifiers;}
    public void setIdentifiers(WorkIdentifiersApiObject[] identifiers) {this.identifiers = identifiers;}

    private HashMap<String, Object> type;
    public HashMap<String, Object> getType() {return type;}
    public void setType(HashMap<String, Object> type) {this.type = type;}

    private HashMap<String, Object> status;
    public HashMap<String, Object> getStatus() {return status;}
    public void setStatus(HashMap<String, Object> status) {this.status = status;}

    private String plannedLaunchDate;
    public String getPlannedLaunchDate() {return plannedLaunchDate;}
    public void setPlannedLaunchDate(String plannedLaunchDate) {this.plannedLaunchDate = plannedLaunchDate;}

    private String actualLaunchDate;
    public String getActualLaunchDate() {return actualLaunchDate;}
    public void setActualLaunchDate(String actualLaunchDate) {this.actualLaunchDate = actualLaunchDate;}

    private String plannedDiscontinuationDate;
    public String getPlannedDiscontinuationDate() {return plannedDiscontinuationDate;}
    public void setPlannedDiscontinuationDate(String plannedDiscontinuationDate) {this.plannedDiscontinuationDate = plannedDiscontinuationDate;}

    private String actualDiscontinuationDate;
    public String getActualDiscontinuationDate() {return actualDiscontinuationDate;}
    public void setActualDiscontinuationDate(String actualDiscontinuationDate) {this.actualDiscontinuationDate = actualDiscontinuationDate;}

    private HashMap<String, Object> imprint;
    public HashMap<String, Object> getImprint() {return imprint;}
    public void setImprint(HashMap<String, Object> imprint) {this.imprint = imprint;}

    private HashMap<String, Object> societyOwnership;
    public HashMap<String, Object> getSocietyOwnership() {return societyOwnership;}
    public void setSocietyOwnership(HashMap<String, Object> societyOwnership) {this.societyOwnership = societyOwnership;}

    private HashMap<String, Object> legalOwnership;
    public HashMap<String, Object> getLegalOwnership() {return legalOwnership;}
    public void setLegalOwnership(HashMap<String, Object> legalOwnership) {this.legalOwnership = legalOwnership;}

    private WorkLegalOwners[] workLegalOwners;
    public WorkLegalOwners[] getWorkLegalOwners() {return workLegalOwners;}
    public void setWorkLegalOwners(WorkLegalOwners[] workLegalOwners) {this.workLegalOwners = workLegalOwners;}

    private WorkAccessModels[] workAccessModels;
    public WorkAccessModels[] getWorkAccessModels() {return workAccessModels;}
    public void setWorkAccessModels(WorkAccessModels[] workAccessModels) {this.workAccessModels = workAccessModels;}

    private WorkBusinessModels[] workBusinessModels;
    public WorkBusinessModels[] getWorkBusinessModels() {return workBusinessModels;}
    public void setWorkBusinessModels(WorkBusinessModels[] workBusinessModels) {this.workBusinessModels = workBusinessModels;}

    private HashMap<String, Object> openAccessType;
    public HashMap<String, Object> getOpenAccessType() {return openAccessType;}
    public void setOpenAccessType(HashMap<String, Object> openAccessType) {this.openAccessType = openAccessType;}

    private PMCApiObject pmc;
    public PMCApiObject getPmc() {return pmc;}
    public void setPmc(PMCApiObject pmc) {this.pmc = pmc;}

    private AccountableProductAPIObject accountableProduct;
    public AccountableProductAPIObject getAccountableProduct() {return accountableProduct;}
    public void setAccountableProduct(AccountableProductAPIObject accountableProduct) {this.accountableProduct = accountableProduct;}

    private FinancialAttributesApiObject[] workFinancialAttributes;
    public FinancialAttributesApiObject[] getWorkFinancialAttributes() {return workFinancialAttributes;}
    public void setWorkFinancialAttributes(FinancialAttributesApiObject[] workFinancialAttributes) {this.workFinancialAttributes = workFinancialAttributes;}

    private PersonsApiObject[] workPersons;
    public PersonsApiObject[] getWorkPersons() {return workPersons;}
    public void setWorkPersons(PersonsApiObject[] workPersons) {this.workPersons = workPersons;}

    private SubjectAreasApiObject[] workSubjectAreas;
    public SubjectAreasApiObject[] getWorkSubjectAreas() {return workSubjectAreas;}
    public void setWorkSubjectAreas(SubjectAreasApiObject[] workSubjectAreas) {this.workSubjectAreas = workSubjectAreas;}

    private WorkRelationshipsAPIObject workRelationships;
    public WorkRelationshipsAPIObject getWorkRelationships() {return workRelationships;}
    public void setWorkRelationships(WorkRelationshipsAPIObject workRelationships) {this.workRelationships = workRelationships;}

    //subjectAreas//EPR-103R9H
    //commented by Nishant @ 13 Apr 2020,yet to implement
    //if(!(this.subjectAreas==null)&&!(this.subjectAreas.length==0)){for (SubjectAreasApiObject sa : this.subjectAreas) {sa.compareWithDB(this.id);}}

    public void compareWithDB(String workId) {
        Log.info("----- Verifying workCore data... " + workId);
        DataQualityContext.breadcrumbMessage = DataQualityContext.breadcrumbMessage + workId;
        getWorkDataFromEPHGD(workId);

        Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - title", title, this.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
        printLog("title");

        if (!(subTitle == null && this.workDataObjectsFromEPHGD.get(0).getWORK_SUBTITLE() == null)) {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - subTitle", subTitle, this.workDataObjectsFromEPHGD.get(0).getWORK_SUBTITLE());
            printLog("subTitle");
        }

        if (electronicRightsInd != null | this.workDataObjectsFromEPHGD.get(0).getELECTRONIC_RIGHTS_IND() != null) {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - electronicRightsInd", Boolean.valueOf(electronicRightsInd), Boolean.valueOf(this.workDataObjectsFromEPHGD.get(0).getELECTRONIC_RIGHTS_IND()));
            printLog("electronicRightsInd");
        }

        if (!(this.workDataObjectsFromEPHGD.get(0).getLANGUAGE_CODE() == null)) {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - language", language.get("code"), this.workDataObjectsFromEPHGD.get(0).getLANGUAGE_CODE());
            printLog("language code");
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - language", language.get("name"), getLanguageName(language.get("code").toString()));
            printLog("language Name");

        }

        //subscription type, implemented by Nishant @ 18 May 2021, EPHD-3122
        if (subscriptionType != null | this.workDataObjectsFromEPHGD.get(0).getSUBSCRIPTION_TYPE() != null) {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - subscriptionType", subscriptionType.get("code"), this.workDataObjectsFromEPHGD.get(0).getSUBSCRIPTION_TYPE());
            printLog("subscriptionType");
            Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - subscriptionName", subscriptionType.get("name"), getSubscriptionName(subscriptionType.get("code").toString()));
            printLog("subscriptionName");

        }

        if (!(editionNumber == null)) {
            int apiEditionNumber = Integer.valueOf(editionNumber);
            Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - EditionNumber", editionNumber, this.workDataObjectsFromEPHGD.get(0).getEDITION_NUMBER());
            printLog("EditionNumber");
        }


        //  int apiVolume =Integer.valueOf(volume);
        //Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - ", " - ",volume, this.workDataObjectsFromEPHGD.get(0).getVOLUME());        printLog("volume");
        //if(Integer.parseInt(this.workDataObjectsFromEPHGD.get(0).getCOPYRIGHT_YEAR())!=0) {

        if (this.workDataObjectsFromEPHGD.get(0).getCOPYRIGHT_YEAR() != null) {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - copyrightYear", copyrightYear, this.workDataObjectsFromEPHGD.get(0).getCOPYRIGHT_YEAR());
            printLog("copyrightYear");
        }

        if (!(identifiers == null && this.workDataObjectsFromEPHGD.get(0).getIDENTIFIER() == null)) {
            Log.info("total identifiers found..." + identifiers.length);
            for (WorkIdentifiersApiObject workIdentifier : identifiers) {
                workIdentifier.compareWithDB();
            }
            printLog("identifiers");
        }

        Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - workType", type.get("code"), this.workDataObjectsFromEPHGD.get(0).getWORK_TYPE());
        printLog("workType code");

        Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - workStatus", status.get("code"), this.workDataObjectsFromEPHGD.get(0).getWORK_STATUS());
        printLog("workStatus code");


        /*
         implemented by Nishant @ 18 May 2021, EPHD-3122
         "plannedLaunchDate": "2020-04-10",
		"actualLaunchDate": "2020-04-11",
		"plannedDiscontinuationDate": "2022-04-24",
		"actualDiscontinueDate"
        */

        if (plannedLaunchDate != null | this.workDataObjectsFromEPHGD.get(0).getPLANNED_LAUNCH_DATE() != null) {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - plannedLanuchDate", plannedLaunchDate, this.workDataObjectsFromEPHGD.get(0).getPLANNED_LAUNCH_DATE());
            printLog("plannedLanuchDate");
        }

        if (actualLaunchDate != null | this.workDataObjectsFromEPHGD.get(0).getACTUAL_LAUNCH_DATE() != null) {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - actualLaunchDate", actualLaunchDate, this.workDataObjectsFromEPHGD.get(0).getACTUAL_LAUNCH_DATE());
            printLog("actualLaunchDate");
        }

        if (plannedDiscontinuationDate != null | this.workDataObjectsFromEPHGD.get(0).getPLANNED_DISCONTINUE_DATE() != null) {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - plannedDiscontinuationDate", plannedDiscontinuationDate, this.workDataObjectsFromEPHGD.get(0).getPLANNED_DISCONTINUE_DATE());
            printLog("plannedDiscontinuationDate");
        }

        if (actualDiscontinuationDate != null | this.workDataObjectsFromEPHGD.get(0).getACTUAL_DISCONTINUE_DATE() != null) {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - actualDiscontinuationDate", actualDiscontinuationDate, this.workDataObjectsFromEPHGD.get(0).getACTUAL_DISCONTINUE_DATE());
            printLog("actualDiscontinuationDate");
        }

        if (!(imprint == null && this.workDataObjectsFromEPHGD.get(0).getIMPRINT() == null)) {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - imprint", imprint.get("code"), this.workDataObjectsFromEPHGD.get(0).getIMPRINT());
            printLog("imprint code");
        }

        /*
        implemented by Nishant @ 18 May 2021, EPHD-3122
        //societyOwnership, if not null -EPR-W-101055
        //legalOwnership, if not null -EPR-W-101055
        //workLegalOwners, if not null -EPR-W-101055

        */

        if (societyOwnership != null | this.workDataObjectsFromEPHGD.get(0).getSOCIETY_OWNERSHIP() != null) {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - societyOwnership", societyOwnership.get("code"), this.workDataObjectsFromEPHGD.get(0).getSOCIETY_OWNERSHIP());
            printLog("societyOwnership code");

            Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - societyOwnership", societyOwnership.get("name"), getSocietyOwnershipName(societyOwnership.get("code").toString()));
            printLog("societyOwnership Name");

            Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - societyOwnership", societyOwnership.get("ownershipRollUp"), getSocietyOwnershipRollUp(societyOwnership.get("code").toString()));
            printLog("societyOwnership Rollup");
        }

        if (legalOwnership != null | this.workDataObjectsFromEPHGD.get(0).getLEGAL_OWNERSHIP() != null) {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - legalOwnership", legalOwnership.get("code"), this.workDataObjectsFromEPHGD.get(0).getLEGAL_OWNERSHIP());
            printLog("legalOwnership code");

            String[] lov = getLegalOwnershipValue(legalOwnership.get("code").toString());
            Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - legalOwnership", legalOwnership.get("name"), lov[0]);
            printLog("legalOwnership Name");

            Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - legalOwnership", legalOwnership.get("ownershipRollUp"), lov[1]);
            printLog("legalOwnership Rollup");
        }

        if (this.workLegalOwners != null) {
            Log.info("total workLegalOwner - " + workLegalOwners.length);
            for (WorkLegalOwners workLegalOwner : workLegalOwners) {workLegalOwner.compareWithDB(workId);}
        }

          /*
        implemented by Nishant @ 19 May 2021, EPHD-3122
        //workAccessModels, if not null -EPR-W-101055
        //workBusinessModels, if not null -EPR-W-101055
        //workSubjectAreas, if not null -EPR-W-101055
        */

        if (workAccessModels != null){
            Log.info("verifying workAccessModel...");
            List<Map<String,Object>> workAccessModelsDB = getWorkAccessModelsId(workId);

            Assert.assertEquals(DataQualityContext.breadcrumbMessage+" - workAccessModels count",workAccessModels.length,workAccessModelsDB.size());

            ArrayList<WorkAccessModels> accessModel_api = new ArrayList(Arrays.asList(workAccessModels));

            for(int cnt = 0;cnt<accessModel_api.size();cnt++ )
            {
                boolean accModelFound = false;
                for(int i=0;i<workAccessModelsDB.size();i++)
                {
                    if(accessModel_api.get(cnt).getAccessModel().get("code").equals(workAccessModelsDB.get(i).get("f_access_model").toString())) {

                        printLog("workAccessModel code "+accessModel_api.get(cnt).getAccessModel().get("code"));
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage+" - workAccessModel ",accessModel_api.get(cnt).getAccessModel().get("name"),getWorkAccessModelName(workAccessModelsDB.get(i).get("f_access_model").toString()));
                        printLog("workAccessModel name "+accessModel_api.get(cnt).getAccessModel().get("name"));

                         if(accessModel_api.get(cnt).getEffectiveStartDate()!=null)
                         {
                             Log.info("workAccessModel effectiveStartDate present");
                             //need to write validation rule
                             Assert.assertTrue(false);

                         }
                        if(accessModel_api.get(cnt).getEffectiveEndDate()!=null)
                        {
                            Log.info("workAccessModel EffectiveEndDate present");
                            //need to write validation rule
                            Assert.assertTrue(false);
                        }

                        accModelFound = true;
                        break;
                    }
                }
                Assert.assertTrue(DataQualityContext.breadcrumbMessage+" - workAccessModel " +accessModel_api.get(cnt).getAccessModel().get("code")+" found in DB - ",accModelFound);
            }
        }

        if (workBusinessModels != null){
            Log.info("verifying workBusinessModels...");
            List<Map<String,Object>> workBusinessModelsDB = getWorkBusinessModelsId(workId);

            Assert.assertEquals(DataQualityContext.breadcrumbMessage+" - workBusinessModels count",workBusinessModels.length,workBusinessModelsDB.size());

            ArrayList<WorkBusinessModels> businessModel_api = new ArrayList(Arrays.asList(workBusinessModels));

            for(int cnt = 0;cnt<businessModel_api.size();cnt++ )
            {
                boolean busModelFound = false;
                for(int i=0;i<workBusinessModelsDB.size();i++)
                {
                    if(businessModel_api.get(cnt).getBusinessModel().get("code").equals(workBusinessModelsDB.get(i).get("f_business_model").toString())) {

                        printLog("workBusinessModel code"+businessModel_api.get(cnt).getBusinessModel().get("code"));
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage+" - workBusinessModel ", businessModel_api.get(cnt).getBusinessModel().get("name"),getWorkBusinessModelName(workBusinessModelsDB.get(i).get("f_business_model").toString()));
                        printLog("workBusinessModel name "+businessModel_api.get(cnt).getBusinessModel().get("name"));

                        if(businessModel_api.get(cnt).getEffectiveStartDate()!=null)
                        {
                            Assert.assertEquals(DataQualityContext.breadcrumbMessage+" - workBusinessModel ",businessModel_api.get(cnt).getEffectiveStartDate(),workBusinessModelsDB.get(i).get("effective_start_date").toString());
                            printLog("workBusinessModel effectiveStartDate present");
                        }
                        if(businessModel_api.get(cnt).getEffectiveEndDate()!=null)
                        {
                            Assert.assertEquals(DataQualityContext.breadcrumbMessage+" - workBusinessModel ",businessModel_api.get(cnt).getEffectiveEndDate(),workBusinessModelsDB.get(i).get("effective_end_date").toString());
                            printLog("workBusinessModel effectiveEndtDate present");
                        }

                        busModelFound = true;
                        break;
                    }
                }
                Assert.assertTrue(DataQualityContext.breadcrumbMessage+" - workBusinessModel " +businessModel_api.get(cnt).getBusinessModel().get("code")+" found in DB - ",busModelFound);
            }
        }

        Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - openAccessType",openAccessType.get("code"), this.workDataObjectsFromEPHGD.get(0).getOPEN_ACCESS_TYPE());
        printLog("openAccessType code");

        Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - pmc",pmc.getCode(), this.workDataObjectsFromEPHGD.get(0).getPMC());
        printLog("pmc code");

        Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - pmg",pmc.getPmg().get("code"), getPMGcodeByPMC(this.workDataObjectsFromEPHGD.get(0).getPMC()));
        printLog("pmg code");
        if (this.workDataObjectsFromEPHGD.get(0).getF_accountable_product() != null) {
            //accountable products varification implemmented by Nishant on 22 Apr 2020
            Log.info("workType - " + type.get("code"));
            Log.info("verifying accountable product id..." + this.workDataObjectsFromEPHGD.get(0).getF_accountable_product());
            getAccountableProductFromEPHGD(this.workDataObjectsFromEPHGD.get(0).getF_accountable_product());

            Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - getGlProductSegmentCode",accountableProduct.getGlProductSegmentCode(), this.accountableProductDataObjectsFromEPHGD.get(0).getGL_PRODUCT_SEGMENT_CODE());
            printLog("getGlProductSegmentCode");

            Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - getGlProductSegmentName",accountableProduct.getGlProductSegmentName(), this.accountableProductDataObjectsFromEPHGD.get(0).getGL_PRODUCT_SEGMENT_NAME());
            printLog("getGlProductSegmentName");

            Assert.assertEquals(DataQualityContext.breadcrumbMessage+ " - getGlProductParentValue",accountableProduct.getGlProductParentValue().get("code"), this.accountableProductDataObjectsFromEPHGD.get(0).getF_GL_PRODUCT_SEGMENT_PARENT());
            printLog("getGlProductParentValue");
        }

        if (!(workFinancialAttributes == null) && !(workFinancialAttributes.length == 0)) {
            for (FinancialAttributesApiObject attribute : workFinancialAttributes) {
                attribute.compareWithDB(workId);
            }
            printLog("workFinancialAttributes");
        }

        if (!(workPersons == null) && !(workPersons.length == 0)) {
            Log.info("total workPersons found..."+workPersons.length);
            for (PersonsApiObject person : workPersons) {
                person.compareWithDB_work(workId);
            }
            printLog("workPersons");
        }

//workSubjectAreas, if not null -EPR-W-101055

        if(workSubjectAreas!=null)
        {
            Log.info("verifying WorkSubjectAreas...total count "+workSubjectAreas.length);
            boolean subAreaFound = false;
            List<Map<String,Object>> workSubjectAreasDB =  getWorkSubjectAreasByWorkId(workId);

            Assert.assertEquals(DataQualityContext.breadcrumbMessage+" workSubjectAreas count", workSubjectAreas.length,workSubjectAreasDB.size());

            for(SubjectAreasApiObject subAreas:workSubjectAreas)
            {
                for(int i=0;i<workSubjectAreasDB.size();i++)
                {
                 if(subAreas.getSubjectArea().getName().equals(workSubjectAreasDB.get(i).get("name")))
                 {  subAreaFound = true;
                    subAreas.compareWithDB(workId);break;
                 }
                }
                Assert.assertTrue(DataQualityContext.breadcrumbMessage+" workSubjectArea found",subAreaFound);
            }
        }
        //The data is stored in table semarchy_eph_mdm.gd_work_relationship.  //workRelationships - EPR-11119M
        if (workRelationships != null) {
            workRelationships.compareWithDB(this.workDataObjectsFromEPHGD.get(0).getWORK_ID());
            printLog("workRelationships");
        }
    }

    private void getWorkDataFromEPHGD(String workID) {
        List<String> ids = new ArrayList<>();
        ids.add(workID);
        String sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_FOR_SEARCH, Joiner.on("','").join(ids));
        workDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    }

    private String getPMGcodeByPMC(String pmcCode) {
        String sql = String.format(APIDataSQL.EPH_GD_PMG_CODE_EXTRACT_BYPMC, pmcCode);
        List<Map<String, Object>> getPMG = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        String pmgCode = ((String) getPMG.get(0).get("f_pmg"));
        return pmgCode;
    }

    private void getAccountableProductFromEPHGD(String accountable_product_id) {
        String sql = String.format(APIDataSQL.SELECT_ACCOUNTABLE_PRODUCT_BY_ACCOUNTABLEID, accountable_product_id);
        accountableProductDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, AccountableProductDataObject.class, Constants.EPH_URL);
    }

    private String getLanguageName(String code){//created by Nishant @ 18 May 2021, EPHD-3122
        String sql = String.format(APIDataSQL.SelectLovLanguageDescription,code);
        List<Map<String,Object>> languageName = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        return (String) languageName.get(0).get("l_description");
    }

    private String getSubscriptionName(String code){//created by Nishant @ 18 May 2021, EPHD-3122
        String sql = String.format(APIDataSQL.SelectLovSubscriptionDescription,code);
        List<Map<String,Object>> subscriptionName = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        return (String) subscriptionName.get(0).get("l_description");
    }

    private String getSocietyOwnershipName(String code){//created by Nishant @ 18 May 2021, EPHD-3122
        String sql = String.format(APIDataSQL.SelectLovsocietyOwnershipValue,code);
        List<Map<String,Object>> societyOwnershipValue = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        return (String)societyOwnershipValue.get(0).get("l_description");
    }

    private String getSocietyOwnershipRollUp(String code){//created by Nishant @ 18 May 2021, EPHD-3122
        String sql = String.format(APIDataSQL.SelectLovsocietyOwnershipValue,code);
        List<Map<String,Object>> societyOwnershipValue = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        return (String)societyOwnershipValue.get(0).get("roll_up_ownership");
    }

    private String[] getLegalOwnershipValue(String code){//created by Nishant @ 18 May 2021, EPHD-3122
        String sql = String.format(APIDataSQL.SelectLovLegalOwnershipValue,code);
        List<Map<String,Object>> legalOwnershipValue = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        String[] arr_lov = {(String)legalOwnershipValue.get(0).get("l_description"),(String)legalOwnershipValue.get(0).get("roll_up_ownership")};
        return arr_lov;
    }

    private List<Map<String,Object>> getWorkAccessModelsId(String workId){//created by Nishant @ 19 May 2021
        String sql = APIDataSQL.getWorkAccessModelsById.replace("PARAM1", workId);
        return  DBManager.getDBResultMap(sql,Constants.EPH_URL);
    }

    private String getWorkAccessModelName(String code) {
        switch(code)
        {
            case "FR":return "Free";
            case "OP":return "Open";
            case "PD":return "Paid";
            default:throw new IllegalArgumentException("Illegal argument: " + code);
        }
    }

    private String getWorkBusinessModelName(String code) {
        switch(code)
        {
            case "SBS":return "Subscription";
            case "SBD":return "Subsidized";
            case "APC":return "Article Publishing Charge";
            default:throw new IllegalArgumentException("Illegal argument: " + code);
        }
    }

    private List<Map<String,Object>> getWorkBusinessModelsId(String workId){//created by Nishant @ 19 May 2021
        String sql = APIDataSQL.getWorkBusinessModelById.replace("PARAM1", workId);
        return  DBManager.getDBResultMap(sql,Constants.EPH_URL);
    }

    private List<Map<String,Object>> getWorkSubjectAreasByWorkId(String workId){//created by Nishant @ 20 May 2021
        String sql = APIDataSQL.getWorkSubjectAreaByWorkId.replace("PARAM1", workId);
        return  DBManager.getDBResultMap(sql,Constants.EPH_URL);
    }
    private void printLog(String verified) {Log.info("verified..." + verified);}

}

