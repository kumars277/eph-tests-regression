package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 */
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;
import org.junit.Assert;
import org.omg.CORBA.PUBLIC_MEMBER;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
class ManifestationWorkApiObject {
    public ManifestationWorkApiObject() {}
    private List<WorkDataObject> workDataObjectsFromEPHGD;

    private String id;
    public String getId() {return id;}
    public void setId(String id) {this.id = id;}

    private workCore workCore;
    public ManifestationWorkApiObject.workCore getWorkCore() {return workCore;}
    public void setWorkCore(ManifestationWorkApiObject.workCore workCore) {this.workCore = workCore;}

    public class workCore{
        private String title;
        public String getTitle() {return title;}
        public void setTitle(String title) {this.title = title;}

        private String subTitle;
        public String getSubTitle() {return subTitle;}
        public void setSubTitle(String subTitle) {this.subTitle = subTitle;}

        private Boolean electronicRightsInd;
        public Boolean getElectronicRightsInd() {return electronicRightsInd;}
        public void setElectronicRightsInd(Boolean electronicRightsInd) {this.electronicRightsInd = electronicRightsInd;}

        private HashMap<String, Object> language;
        public HashMap<String, Object> getLanguage() {return language;}
        public void setLanguage(HashMap<String, Object> language) {this.language = language;}

        private String editionNumber;
        public String getEditionNumber() {return editionNumber;}
        public void setEditionNumber(String editionNumber) {this.editionNumber = editionNumber;}

        private HashMap<String, Object> subscriptionType;
        public HashMap<String, Object> getSubscriptionType() {return subscriptionType;}
        public void setsubscriptionType(HashMap<String, Object> subscriptionType) {this.subscriptionType = subscriptionType;}

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

        private HashMap<String, Object> imprint;
        public HashMap<String, Object> getImprint() {return imprint;}
        public void setImprint(HashMap<String, Object> imprint) {this.imprint = imprint;}

        private HashMap<String, Object> societyOwnership;
        public HashMap<String, Object> getSocietyOwnership() {return societyOwnership;}
        public void setSocietyOwnership(HashMap<String, Object> societyOwnership) {this.societyOwnership = societyOwnership;}

        private HashMap<String, Object> openAccessType;
        public HashMap<String, Object> getOpenAccessType() {return openAccessType;}
        public void setOpenAccessType(HashMap<String, Object> openAccessType) {this.openAccessType = openAccessType;}

        private PMCApiObject pmc;
        public PMCApiObject getPmc() {return pmc;}
        public void setPmc(PMCApiObject pmc) {this.pmc = pmc;}

        private  AccountableProductAPIObject accountableProduct;
        public AccountableProductAPIObject getAccountableProduct() {return accountableProduct;}
        public void setAccountableProduct(AccountableProductAPIObject accountableProduct) {this.accountableProduct = accountableProduct;}

        private FinancialAttributesApiObject[] workFinancialAttributes;
        public FinancialAttributesApiObject[] getWorkFinancialAttributes() {return workFinancialAttributes;}
        public void setWorkFinancialAttributes(FinancialAttributesApiObject[] workFinancialAttributes) {this.workFinancialAttributes = workFinancialAttributes;}


        private PersonsApiObject[] workPersons;
        public PersonsApiObject[] getWorkPersons() {return workPersons;}
        public void setWorkPersons(PersonsApiObject[] workPersons) {this.workPersons = workPersons;}

        //subjectAreas//EPR-103R9H
    }



    public void compareWithDB(){
        getWorkDataFromEPHGD(this.id);
        Log.info("comparing work id..."+this.id);
        Log.info("script in progress");
        Assert.assertEquals(workCore.title, this.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
        Assert.assertEquals(workCore.subTitle, this.workDataObjectsFromEPHGD.get(0).getWORK_SUBTITLE());
        Assert.assertEquals(Boolean.valueOf(workCore.electronicRightsInd), Boolean.valueOf(this.workDataObjectsFromEPHGD.get(0).getELECTRONIC_RIGHTS_IND()));
        if(!(this.workDataObjectsFromEPHGD.get(0).getLANGUAGE_CODE()==null)) {
            Assert.assertEquals(workCore.language.get("code"), this.workDataObjectsFromEPHGD.get(0).getLANGUAGE_CODE());
        }
        if(!(workCore.editionNumber==null)){
            int apiEditionNumber = Integer.valueOf(workCore.editionNumber);
            Assert.assertEquals(apiEditionNumber, this.workDataObjectsFromEPHGD.get(0).getEDITION_NUMBER());
        }

        int apiVolume =Integer.valueOf(workCore.volume);
        Assert.assertEquals(apiVolume, this.workDataObjectsFromEPHGD.get(0).getVOLUME());

        if(this.workDataObjectsFromEPHGD.get(0).getCOPYRIGHT_YEAR()!=0) {
            int apiCopyrightYear = Integer.valueOf(workCore.copyrightYear);
            Assert.assertEquals(apiCopyrightYear, this.workDataObjectsFromEPHGD.get(0).getCOPYRIGHT_YEAR());
        }
        Assert.assertEquals(workCore.type.get("code"), this.workDataObjectsFromEPHGD.get(0).getWORK_TYPE());
        Assert.assertEquals(workCore.status.get("code"), this.workDataObjectsFromEPHGD.get(0).getWORK_STATUS());
        if(!(workCore.imprint.get("code")==null)||!((this.workDataObjectsFromEPHGD==null)||(this.workDataObjectsFromEPHGD.isEmpty()))) {
            Assert.assertEquals(workCore.imprint.get("code"), this.workDataObjectsFromEPHGD.get(0).getIMPRINT());
        }
        Assert.assertEquals(workCore.openAccessType.get("code"),this.workDataObjectsFromEPHGD.get(0).getOPEN_ACCESS_TYPE());
        Assert.assertEquals(workCore.pmc.getCode(), this.workDataObjectsFromEPHGD.get(0).getPMC());
        Assert.assertEquals(workCore.pmc.getPmg().get("code"), getPMGcodeByPMC(this.workDataObjectsFromEPHGD.get(0).getPMC()));
        if(!(workCore.workFinancialAttributes==null)&&!(workCore.workFinancialAttributes.length==0)){
            for (FinancialAttributesApiObject attribute : workCore.workFinancialAttributes) {attribute.compareWithDB(this.id);}
        }

        if(!(workCore.workPersons==null)&&!(workCore.workPersons.length==0)){
            for (PersonsApiObject person : workCore.workPersons) {person.compareWithDB_work();}
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
        //Log.info(sql);
        List<Map<String, Object>> getPMG = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        String pmgCode = ((String) getPMG.get(0).get("f_pmg"));
        return pmgCode;
    }

}