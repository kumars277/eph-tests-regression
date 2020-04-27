package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 * updated by Nishant @ 30 Mar 2020 as per latest API changes
 * moving all relevant tags inside newly added workCore object
 */
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.dao.AccountableProductDataObject;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkApiObject {

    public WorkApiObject() {}

    private List<WorkDataObject> workDataObjectsFromEPHGD;
    public List<WorkDataObject> getWorkDataObjectsFromEPHGD() {return workDataObjectsFromEPHGD;}
    public void setWorkDataObjectsFromEPHGD(List<WorkDataObject> workDataObjectsFromEPHGD) {this.workDataObjectsFromEPHGD = workDataObjectsFromEPHGD;}

    private List<AccountableProductDataObject> accountableProductDataObjectsFromEPHGD;
    public List<AccountableProductDataObject> getAccountableProductDataObjectsFromEPHGD() {return accountableProductDataObjectsFromEPHGD;}
    public void setAccountableProductDataObjectsFromEPHGD(List<AccountableProductDataObject> accountableProductDataObjectsFromEPHGD) {this.accountableProductDataObjectsFromEPHGD = accountableProductDataObjectsFromEPHGD;}

    private String createdDate;
    public String getCreatedDate(){return  createdDate;}
    public void setCreatedDate(String createdDate) {this.createdDate = createdDate;}

    private String updatedDate;
    public String getUpdatedDate(){return updatedDate;}
    public void setUpdatedDate(String updatedDate) {this.updatedDate = updatedDate;}

    private String id;
    public String getId() {return id;}
    public void setId(String id) {this.id = id;}

    private workcore workCore;
    public workcore getWorkCore() {return workCore;}
    public void setWorkCore(workcore workCore) {this.workCore = workCore;}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public class workcore{
        private String title;
        public String getTitle() {return title;}
        public void setTitle(String title) {this.title = title;}

        private String subTitle;
        public String getSubTitle() {return subTitle;}
        public void setSubTitle(String subTitle) {this.subTitle = subTitle;}

        private String electronicRightsInd;
        public String getElectronicRightsInd() {return electronicRightsInd;}
        public void setElectronicRightsInd(String electronicRightsInd) {this.electronicRightsInd = electronicRightsInd;}

        private HashMap<String,Object> language;
        public HashMap<String, Object> getLanguage() {return language;}
        public void setLanguage(HashMap<String, Object> language) {this.language = language;}

        private String editionNumber;
        public String getEditionNumber() {return editionNumber;}
        public void setEditionNumber(String editionNumber) {this.editionNumber = editionNumber;}

        private String volume;
        public String getVolume() {return volume;}
        public void setVolume(String volume) {this.volume = volume;}

        private String copyrightYear;
        public String getCopyrightYear() {return copyrightYear;}
        public void setCopyrightYear(String copyrightYear) {this.copyrightYear = copyrightYear;}

        private WorkIdentifiersApiObject[] identifiers;
        public WorkIdentifiersApiObject[] getIdentifiers() {return identifiers;}
        public void setIdentifiers(WorkIdentifiersApiObject[] identifiers) {this.identifiers = identifiers;}

        private HashMap<String,Object> type;
        public HashMap<String, Object> getType() {return type;}
        public void setType(HashMap<String, Object> type) {this.type = type;}

        private HashMap<String,Object> status;
        public HashMap<String, Object> getStatus() {return status;}
        public void setStatus(HashMap<String, Object> status) {this.status = status;}

        private HashMap<String,Object> imprint;
        public HashMap<String, Object> getImprint() {return imprint;}
        public void setImprint(HashMap<String, Object> imprint) {this.imprint = imprint;}

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

      }

    private WorkManifestationApiObject[] manifestations;
    public WorkManifestationApiObject[] getManifestations() {return manifestations;}
    public void setManifestations(WorkManifestationApiObject[] manifestations) {this.manifestations = manifestations;}

    //created by Nishant @ 17 Apr 2020
    private List<ManifestationProductAPIObject> products;
    public List<ManifestationProductAPIObject> getProducts() {return products;}
    public void setProducts(List<ManifestationProductAPIObject> products) {this.products = products;}

    public void compareWithDB(){
        Log.info("comparing below with DB for work... "+this.id+ "\n-title \n-subTitle \n-electronicRightsInd \n"
                        +"-language \n-editionNumber \n-volume \n-copyrightYear \n-identifiers");

        getWorkDataFromEPHGD(this.id);
        Assert.assertEquals(workCore.title, this.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
        Assert.assertEquals(workCore.subTitle,this.workDataObjectsFromEPHGD.get(0).getWORK_SUBTITLE());
        Assert.assertEquals(Boolean.valueOf(workCore.electronicRightsInd), Boolean.valueOf(this.workDataObjectsFromEPHGD.get(0).getELECTRONIC_RIGHTS_IND()));
        if(!(this.workDataObjectsFromEPHGD.get(0).getLANGUAGE_CODE()==null)) {
            Assert.assertEquals(workCore.language.get("code"), this.workDataObjectsFromEPHGD.get(0).getLANGUAGE_CODE());}
        if(!(workCore.editionNumber==null)){int apiEditionNumber = Integer.valueOf(workCore.editionNumber);
            Assert.assertEquals(apiEditionNumber, this.workDataObjectsFromEPHGD.get(0).getEDITION_NUMBER());}

        int apiVolume =Integer.valueOf(workCore.volume);
        Assert.assertEquals(apiVolume, this.workDataObjectsFromEPHGD.get(0).getVOLUME());

        if(this.workDataObjectsFromEPHGD.get(0).getCOPYRIGHT_YEAR()!=0) {
            int apiCopyrightYear = Integer.valueOf(workCore.copyrightYear);
            Assert.assertEquals(apiCopyrightYear, this.workDataObjectsFromEPHGD.get(0).getCOPYRIGHT_YEAR());}

        if(workCore.identifiers!=null) {for (WorkIdentifiersApiObject workIdentifier : workCore.identifiers) {workIdentifier.compareWithDB();}}

        Log.info("comparing...\n-work type \n-work status \n-imprint \n-openAccessType \n-pmc code \n-pmg code");
        Log.info("workType - "+workCore.type.get("code"));
        Assert.assertEquals(workCore.type.get("code"), this.workDataObjectsFromEPHGD.get(0).getWORK_TYPE());
        Assert.assertEquals(workCore.status.get("code"), this.workDataObjectsFromEPHGD.get(0).getWORK_STATUS());
        if(!(workCore.imprint.get("code")==null)||!((this.workDataObjectsFromEPHGD==null)||(this.workDataObjectsFromEPHGD.isEmpty()))) {
           Assert.assertEquals(workCore.imprint.get("code"), this.workDataObjectsFromEPHGD.get(0).getIMPRINT());}

        Assert.assertEquals(workCore.openAccessType.get("code"),this.workDataObjectsFromEPHGD.get(0).getOPEN_ACCESS_TYPE());
        Assert.assertEquals(workCore.pmc.getCode(), this.workDataObjectsFromEPHGD.get(0).getPMC());
        Assert.assertEquals(workCore.pmc.getPmg().get("code"), getPMGcodeByPMC(this.workDataObjectsFromEPHGD.get(0).getPMC()));
        if(this.workDataObjectsFromEPHGD.get(0).getF_accountable_product()!=null) {
            //accountable products varification implemmented by Nishant on 22 Apr 2020
            Log.info("comparing accountable product id..." + this.workDataObjectsFromEPHGD.get(0).getF_accountable_product());
            getAccountableProductFromEPHGD(this.workDataObjectsFromEPHGD.get(0).getF_accountable_product());
            Log.info("getGlProductSegmentCode - " + workCore.accountableProduct.getGlProductSegmentCode());
            Assert.assertEquals(workCore.accountableProduct.getGlProductSegmentCode(), this.accountableProductDataObjectsFromEPHGD.get(0).getGL_PRODUCT_SEGMENT_CODE());
            Log.info("glProductSegmentName - " + workCore.accountableProduct.getGlProductSegmentName());
            Assert.assertEquals(workCore.accountableProduct.getGlProductSegmentName(), this.accountableProductDataObjectsFromEPHGD.get(0).getGL_PRODUCT_SEGMENT_NAME());
            Log.info("glProductParentValue_code - " + workCore.accountableProduct.getGlProductParentValue().get("code"));
            Assert.assertEquals(workCore.accountableProduct.getGlProductParentValue().get("code"), this.accountableProductDataObjectsFromEPHGD.get(0).getF_GL_PRODUCT_SEGMENT_PARENT());

        }
                if(!(workCore.workFinancialAttributes==null)&&!(workCore.workFinancialAttributes.length==0)){
            for (FinancialAttributesApiObject attribute : workCore.workFinancialAttributes) {attribute.compareWithDB(this.id);}}
        if(!(workCore.workPersons==null)&&!(workCore.workPersons.length==0)){
            for (PersonsApiObject person : workCore.workPersons) {person.compareWithDB_work();}}
        //commented by Nishant @ 13 Apr 2020,yet to implement
        //if(!(this.subjectAreas==null)&&!(this.subjectAreas.length==0)){for (SubjectAreasApiObject sa : this.subjectAreas) {sa.compareWithDB(this.id);}}
        for (WorkManifestationApiObject manifestation : manifestations){manifestation.compareWithDB();}

        //implemented by Nishant @ 23 Apr 2020
      if(products!=null) {
          Log.info("comparing work products...");
          for (ManifestationProductAPIObject Workproducts : products) {
              Workproducts.compareWithDB();
          }
      }
    }

    private String getPMGcodeByPMC(String pmcCode) {
        String sql = String.format(APIDataSQL.EPH_GD_PMG_CODE_EXTRACT_BYPMC, pmcCode);
        //  Log.info(sql);
        List<Map<String, Object>> getPMG = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        String pmgCode = ((String) getPMG.get(0).get("f_pmg"));
        return pmgCode;
    }

    private void getWorkDataFromEPHGD(String workID) {
        List<String> ids = new ArrayList<>();
        ids.add(workID);
        String sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_FOR_SEARCH, Joiner.on("','").join(ids));
        workDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    }

    private void getAccountableProductFromEPHGD(String accountable_product_id)
    {
        String sql=String.format(APIDataSQL.SELECT_ACCOUNTABLE_PRODUCT_BY_ACCOUNTABLEID,accountable_product_id);
        accountableProductDataObjectsFromEPHGD=DBManager.getDBResultAsBeanList(sql, AccountableProductDataObject.class,Constants.EPH_URL);
    }


    //As of 30 mar 2020, per latest changes, below tags are not available in getwork API
    /*
    private CopyrightOwnersApiObject[] copyrightOwner;
    private SubjectAreasApiObject[] subjectAreas;

    public SubjectAreasApiObject[] getSubjectAreas() {
        return subjectAreas;
    }
    public void setSubjectAreas(SubjectAreasApiObject[] subjectAreas) {
        this.subjectAreas = subjectAreas;
    }

    public CopyrightOwnersApiObject[] getCopyrightOwner() {
        return copyrightOwner;
    }
    public void setCopyrightOwner(CopyrightOwnersApiObject[] copyrightOwner) {
        this.copyrightOwner = copyrightOwner;
    }
*/

}