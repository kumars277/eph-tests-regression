package com.eph.automation.testing.models.api;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.dao.AccountableProductDataObject;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;
import net.minidev.json.parser.ParseException;
import org.junit.Assert;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        getWorkDataFromEPHGD(workId);

        Assert.assertEquals(workId + " - ", title, this.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
        printLog("title");

        if (!(subTitle == null && this.workDataObjectsFromEPHGD.get(0).getWORK_SUBTITLE() == null)) {
            Assert.assertEquals(workId + " - ", subTitle, this.workDataObjectsFromEPHGD.get(0).getWORK_SUBTITLE());
            printLog("subTitle");
        }

        if (electronicRightsInd != null | this.workDataObjectsFromEPHGD.get(0).getELECTRONIC_RIGHTS_IND() != null) {
            Assert.assertEquals(workId + " - ", Boolean.valueOf(electronicRightsInd), Boolean.valueOf(this.workDataObjectsFromEPHGD.get(0).getELECTRONIC_RIGHTS_IND()));
            printLog("electronicRightsInd");
        }

        if (!(this.workDataObjectsFromEPHGD.get(0).getLANGUAGE_CODE() == null)) {
            Assert.assertEquals(workId + " - ", language.get("code"), this.workDataObjectsFromEPHGD.get(0).getLANGUAGE_CODE());
            printLog("language code");
            Assert.assertEquals(workId + " - ", language.get("name"), getLanguageName(language.get("code").toString()));
            printLog("language Name");

        }

        //subscription type, implemented by Nishant @ 18 May 2021, EPHD-3122
        if (subscriptionType != null | this.workDataObjectsFromEPHGD.get(0).getSUBSCRIPTION_TYPE() != null) {
            Assert.assertEquals(workId + " - ", subscriptionType.get("code"), this.workDataObjectsFromEPHGD.get(0).getSUBSCRIPTION_TYPE());
            printLog("subscriptionType");
            Assert.assertEquals(workId + " - ", subscriptionType.get("name"), getSubscriptionName(subscriptionType.get("code").toString()));
            printLog("subscriptionName");

        }

        if (!(editionNumber == null)) {
            int apiEditionNumber = Integer.valueOf(editionNumber);
            Assert.assertEquals(workId + " - ", editionNumber, this.workDataObjectsFromEPHGD.get(0).getEDITION_NUMBER());
            printLog("EditionNumber");
        }


        //  int apiVolume =Integer.valueOf(volume);
        //Assert.assertEquals(workId+ " - ",volume, this.workDataObjectsFromEPHGD.get(0).getVOLUME());        printLog("volume");
        //if(Integer.parseInt(this.workDataObjectsFromEPHGD.get(0).getCOPYRIGHT_YEAR())!=0) {

        if (this.workDataObjectsFromEPHGD.get(0).getCOPYRIGHT_YEAR() != null) {
            Assert.assertEquals(workId + " - ", copyrightYear, this.workDataObjectsFromEPHGD.get(0).getCOPYRIGHT_YEAR());
            printLog("copyrightYear");
        }

        if (!(identifiers == null && this.workDataObjectsFromEPHGD.get(0).getIDENTIFIER() == null)) {
            Log.info("total identifiers found..." + identifiers.length);
            for (WorkIdentifiersApiObject workIdentifier : identifiers) {
                workIdentifier.compareWithDB();
            }
            printLog("identifiers");
        }

        Assert.assertEquals(workId + " - ", type.get("code"), this.workDataObjectsFromEPHGD.get(0).getWORK_TYPE());
        printLog("workType code");

        Assert.assertEquals(workId + " - ", status.get("code"), this.workDataObjectsFromEPHGD.get(0).getWORK_STATUS());
        printLog("workStatus code");


        /*
         implemented by Nishant @ 18 May 2021, EPHD-3122
         "plannedLaunchDate": "2020-04-10",
		"actualLaunchDate": "2020-04-11",
		"plannedDiscontinuationDate": "2022-04-24",
		"actualDiscontinueDate"
        */

        if (plannedLaunchDate != null | this.workDataObjectsFromEPHGD.get(0).getPLANNED_LAUNCH_DATE() != null) {
            Assert.assertEquals(workId + " - ", plannedLaunchDate, this.workDataObjectsFromEPHGD.get(0).getPLANNED_LAUNCH_DATE());
            printLog("plannedLanuchDate");
        }

        if (actualLaunchDate != null | this.workDataObjectsFromEPHGD.get(0).getACTUAL_LAUNCH_DATE() != null) {
            Assert.assertEquals(workId + " - ", actualLaunchDate, this.workDataObjectsFromEPHGD.get(0).getACTUAL_LAUNCH_DATE());
            printLog("actualLaunchDate");
        }

        if (plannedDiscontinuationDate != null | this.workDataObjectsFromEPHGD.get(0).getPLANNED_DISCONTINUE_DATE() != null) {
            Assert.assertEquals(workId + " - ", plannedDiscontinuationDate, this.workDataObjectsFromEPHGD.get(0).getPLANNED_DISCONTINUE_DATE());
            printLog("plannedDiscontinuationDate");
        }

        if (actualDiscontinuationDate != null | this.workDataObjectsFromEPHGD.get(0).getACTUAL_DISCONTINUE_DATE() != null) {
            Assert.assertEquals(workId + " - ", actualDiscontinuationDate, this.workDataObjectsFromEPHGD.get(0).getACTUAL_DISCONTINUE_DATE());
            printLog("actualDiscontinuationDate");
        }

        if (!(imprint == null && this.workDataObjectsFromEPHGD.get(0).getIMPRINT() == null)) {
            Assert.assertEquals(workId + " - ", imprint.get("code"), this.workDataObjectsFromEPHGD.get(0).getIMPRINT());
            printLog("imprint code");
        }

        /*
        implemented by Nishant @ 18 May 2021, EPHD-3122
        //societyOwnership, if not null -EPR-W-101055
        //legalOwnership, if not null -EPR-W-101055
        //workLegalOwners, if not null -EPR-W-101055
        //workAccessModels, if not null -EPR-W-101055
        //workBusinessModels, if not null -EPR-W-101055
        */

        if (societyOwnership != null | this.workDataObjectsFromEPHGD.get(0).getSOCIETY_OWNERSHIP() != null){
            Assert.assertEquals(workId + " societyOwnership- ", societyOwnership.get("code"), this.workDataObjectsFromEPHGD.get(0).getSOCIETY_OWNERSHIP());
            printLog("societyOwnership code");

            Assert.assertEquals(workId + " societyOwnership- ", societyOwnership.get("name"), getSocietyOwnershipName(societyOwnership.get("code").toString()));
            printLog("societyOwnership Name");

            Assert.assertEquals(workId + " societyOwnership- ", societyOwnership.get("ownershipRollUp"), getSocietyOwnershipRollUp(societyOwnership.get("code").toString()));
            printLog("societyOwnership Rollup");
        }

        if (legalOwnership != null | this.workDataObjectsFromEPHGD.get(0).getLEGAL_OWNERSHIP() != null){
            Assert.assertEquals(workId + " - ", legalOwnership.get("code"), this.workDataObjectsFromEPHGD.get(0).getLEGAL_OWNERSHIP());
            printLog("legalOwnership code");

            String[] lov = getLegalOwnershipValue(legalOwnership.get("code").toString());
            Assert.assertEquals(workId + " - ", legalOwnership.get("name"),lov[0] );
            printLog("legalOwnership Name");

            Assert.assertEquals(workId + " - ", legalOwnership.get("ownershipRollUp"), lov[1]);
            printLog("legalOwnership Rollup");
        }

        if(this.workLegalOwners!=null)
        {
            Log.info("total workLegalOwner - " + workLegalOwners.length);

            for(WorkLegalOwners  workLegalOwner: workLegalOwners)
            {
                workLegalOwner.compareWithDB(workId);
            }
        }



        Assert.assertEquals(workId+ " - ",openAccessType.get("code"), this.workDataObjectsFromEPHGD.get(0).getOPEN_ACCESS_TYPE());
        printLog("openAccessType code");

        Assert.assertEquals(workId+ " - ",pmc.getCode(), this.workDataObjectsFromEPHGD.get(0).getPMC());
        printLog("pmc code");

        Assert.assertEquals(workId+ " - ",pmc.getPmg().get("code"), getPMGcodeByPMC(this.workDataObjectsFromEPHGD.get(0).getPMC()));
        printLog("pmg code");
        if (this.workDataObjectsFromEPHGD.get(0).getF_accountable_product() != null) {
            //accountable products varification implemmented by Nishant on 22 Apr 2020
            Log.info("workType - " + type.get("code"));
            Log.info("verifying accountable product id..." + this.workDataObjectsFromEPHGD.get(0).getF_accountable_product());
            getAccountableProductFromEPHGD(this.workDataObjectsFromEPHGD.get(0).getF_accountable_product());

            Assert.assertEquals(workId+ " - ",accountableProduct.getGlProductSegmentCode(), this.accountableProductDataObjectsFromEPHGD.get(0).getGL_PRODUCT_SEGMENT_CODE());
            printLog("getGlProductSegmentCode");

            Assert.assertEquals(workId+ " - ",accountableProduct.getGlProductSegmentName(), this.accountableProductDataObjectsFromEPHGD.get(0).getGL_PRODUCT_SEGMENT_NAME());
            printLog("getGlProductSegmentName");

            Assert.assertEquals(workId+ " - ",accountableProduct.getGlProductParentValue().get("code"), this.accountableProductDataObjectsFromEPHGD.get(0).getF_GL_PRODUCT_SEGMENT_PARENT());
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
                person.compareWithDB_work();
            }
            printLog("workPersons");
        }

//workSubjectAreas, if not null -EPR-W-101055

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

private String getLanguageName(String code)
{//created by Nishant @ 18 May 2021, EPHD-3122
    String sql = String.format(APIDataSQL.SelectLovLanguageDescription,code);
    List<Map<String,Object>> languageName = DBManager.getDBResultMap(sql,Constants.EPH_URL);
    return (String) languageName.get(0).get("l_description");
}

    private String getSubscriptionName(String code)
    {//created by Nishant @ 18 May 2021, EPHD-3122
        String sql = String.format(APIDataSQL.SelectLovSubscriptionDescription,code);
        List<Map<String,Object>> subscriptionName = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        return (String) subscriptionName.get(0).get("l_description");
    }

    private String getSocietyOwnershipName(String code)
    {//created by Nishant @ 18 May 2021, EPHD-3122
        String sql = String.format(APIDataSQL.SelectLovsocietyOwnershipValue,code);
        List<Map<String,Object>> societyOwnershipValue = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        return (String)societyOwnershipValue.get(0).get("l_description");
    }

    private String getSocietyOwnershipRollUp(String code)
    {//created by Nishant @ 18 May 2021, EPHD-3122
        String sql = String.format(APIDataSQL.SelectLovsocietyOwnershipValue,code);
        List<Map<String,Object>> societyOwnershipValue = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        return (String)societyOwnershipValue.get(0).get("roll_up_ownership");
    }


    private String[] getLegalOwnershipValue(String code)
    {//created by Nishant @ 18 May 2021, EPHD-3122
        String sql = String.format(APIDataSQL.SelectLovLegalOwnershipValue,code);
        List<Map<String,Object>> legalOwnershipValue = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        String[] arr_lov = {(String)legalOwnershipValue.get(0).get("l_description"),(String)legalOwnershipValue.get(0).get("roll_up_ownership")};
        return arr_lov;
    }



    private void printLog(String verified) {
        Log.info("verified..." + verified);
    }

}

