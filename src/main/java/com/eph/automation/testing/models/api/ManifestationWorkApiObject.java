package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 * updated by Nishant @ 4 May 2020
 * updated by Nishant @ 04 Feb 2021, EPHD-2747
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


import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ManifestationWorkApiObject {
    public ManifestationWorkApiObject() {}
    private List<WorkDataObject> workDataObjectsFromEPHGD;

    private String id;
    public String getId() {return id;}
    public void setId(String id) {this.id = id;}

    private workCore workCore;
    public com.eph.automation.testing.models.api.workCore getWorkCore() {return workCore;}
    public void setWorkCore(com.eph.automation.testing.models.api.workCore workCore) {this.workCore = workCore;}

    /*
            private workCore workCore;
            public ManifestationWorkApiObject.workCore getWorkCore() {return workCore;}
            public void setWorkCore(ManifestationWorkApiObject.workCore workCore) {this.workCore = workCore;}
        */
    private List<AccountableProductDataObject> accountableProductDataObjectsFromEPHGD;
    public List<AccountableProductDataObject> getAccountableProductDataObjectsFromEPHGD() {return accountableProductDataObjectsFromEPHGD;}
    public void setAccountableProductDataObjectsFromEPHGD(List<AccountableProductDataObject> accountableProductDataObjectsFromEPHGD) {this.accountableProductDataObjectsFromEPHGD = accountableProductDataObjectsFromEPHGD;}


    public void compareWithDB() throws ParseException {
        getWorkDataFromEPHGD(this.id);
        Log.info("comparing work id..."+this.id);
        Log.info("-title\n-subTitle \n-electronicRightsInd \n-language code \n-editionNumber \n-volume \n-copyrightYear");

        Assert.assertEquals(workCore.getTitle(), this.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
       if(!(workCore.getSubTitle()==null &&this.workDataObjectsFromEPHGD.get(0).getWORK_SUBTITLE()==null))
        Assert.assertEquals(workCore.getSubTitle(), this.workDataObjectsFromEPHGD.get(0).getWORK_SUBTITLE());
        Assert.assertEquals(Boolean.valueOf(workCore.getElectronicRightsInd()), Boolean.valueOf(this.workDataObjectsFromEPHGD.get(0).getELECTRONIC_RIGHTS_IND()));
        if(!(this.workDataObjectsFromEPHGD.get(0).getLANGUAGE_CODE()==null)) {
            Assert.assertEquals(workCore.getLanguage().get("code"), this.workDataObjectsFromEPHGD.get(0).getLANGUAGE_CODE());
        }
        if(!(workCore.getEditionNumber()==null)){
            int apiEditionNumber = Integer.valueOf(workCore.getEditionNumber());
            Assert.assertEquals(apiEditionNumber, this.workDataObjectsFromEPHGD.get(0).getEDITION_NUMBER());
        }

        int apiVolume =Integer.valueOf(workCore.getVolume());
        Assert.assertEquals(apiVolume, this.workDataObjectsFromEPHGD.get(0).getVOLUME());

        if(Integer.parseInt(this.workDataObjectsFromEPHGD.get(0).getCOPYRIGHT_YEAR())!=0) {
            int apiCopyrightYear = Integer.valueOf(workCore.getCopyrightYear());
            Assert.assertEquals(apiCopyrightYear, this.workDataObjectsFromEPHGD.get(0).getCOPYRIGHT_YEAR());
        }

        //IDENTIFIERS - EPR-11119M
        if(!(workCore.getIdentifiers()==null &&this.workDataObjectsFromEPHGD.get(0).getIDENTIFIER()==null) )
            for(WorkIdentifiersApiObject identifier:workCore.getIdentifiers()){identifier.compareWithDB();}
            Log.info( "-work type code \n-work status code \n-imprint \n-openAccessType code \n-pmc \n-pmg\n");
        Assert.assertEquals(workCore.getType().get("code"), this.workDataObjectsFromEPHGD.get(0).getWORK_TYPE());
        Assert.assertEquals(workCore.getStatus().get("code"), this.workDataObjectsFromEPHGD.get(0).getWORK_STATUS());
        if(!(workCore.getImprint().get("code")==null)||!((this.workDataObjectsFromEPHGD==null)||(this.workDataObjectsFromEPHGD.isEmpty()))) {
            Assert.assertEquals(workCore.getImprint().get("code"), this.workDataObjectsFromEPHGD.get(0).getIMPRINT());
        }
        Assert.assertEquals(workCore.getOpenAccessType().get("code"),this.workDataObjectsFromEPHGD.get(0).getOPEN_ACCESS_TYPE());
        Assert.assertEquals(workCore.getPmc().getCode(), this.workDataObjectsFromEPHGD.get(0).getPMC());
        Assert.assertEquals(workCore.getPmc().getPmg().get("code"), getPMGcodeByPMC(this.workDataObjectsFromEPHGD.get(0).getPMC()));

        if(this.workDataObjectsFromEPHGD.get(0).getF_accountable_product()!=null) {
            //accountable products varification implemmented by Nishant on 4 May 2020
            Log.info("work type..."+workCore.getType().get("code"));
            Log.info("comparing accountable product id..." + this.workDataObjectsFromEPHGD.get(0).getF_accountable_product());
            getAccountableProductFromEPHGD(this.workDataObjectsFromEPHGD.get(0).getF_accountable_product());
            Log.info("getGlProductSegmentCode - " + workCore.getAccountableProduct().getGlProductSegmentCode());
            Assert.assertEquals(workCore.getAccountableProduct().getGlProductSegmentCode(), this.accountableProductDataObjectsFromEPHGD.get(0).getGL_PRODUCT_SEGMENT_CODE());
            Log.info("glProductSegmentName - " + workCore.getAccountableProduct().getGlProductSegmentName());
            Assert.assertEquals(workCore.getAccountableProduct().getGlProductSegmentName(), this.accountableProductDataObjectsFromEPHGD.get(0).getGL_PRODUCT_SEGMENT_NAME());
            Log.info("glProductParentValue_code - " + workCore.getAccountableProduct().getGlProductParentValue().get("code"));
            Assert.assertEquals(workCore.getAccountableProduct().getGlProductParentValue().get("code"), this.accountableProductDataObjectsFromEPHGD.get(0).getF_GL_PRODUCT_SEGMENT_PARENT());
        }

        if(!(workCore.getWorkFinancialAttributes()==null)&&!(workCore.getWorkFinancialAttributes().length==0)){
            for (FinancialAttributesApiObject attribute : workCore.getWorkFinancialAttributes()) {attribute.compareWithDB(this.id);}
        }

        if(!(workCore.getWorkPersons()==null)&&!(workCore.getWorkPersons().length==0)){
            for (PersonsApiObject person : workCore.getWorkPersons()) {person.compareWithDB_work(id);}
        }

        //workRelationships - EPR-11119M
        //The data is stored in table semarchy_eph_mdm.gd_work_relationship.
        if(workCore.getWorkRelationships()!=null)workCore.getWorkRelationships().compareWithDB(this.workDataObjectsFromEPHGD.get(0).getWORK_ID());
    }

    private void getWorkDataFromEPHGD(String workID) {
        List<String> ids = new ArrayList<>();
        ids.add(workID);
        String sql = String.format(APIDataSQL.GET_GD_DATA_WORK, Joiner.on("','").join(ids));
        workDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    }

    private String getPMGcodeByPMC(String pmcCode) {
        String sql = String.format(APIDataSQL.SELECT_GD_PMG_BY_PMC, pmcCode);
        //Log.info(sql);
        List<Map<String, Object>> getPMG = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        String pmgCode = ((String) getPMG.get(0).get("f_pmg"));
        return pmgCode;
    }

    private void getAccountableProductFromEPHGD(String accountable_product_id)
    {
        String sql=String.format(APIDataSQL.GET_GD_DATA_ACCOUNTABLEPRODUCT_BY_ID,accountable_product_id);
        accountableProductDataObjectsFromEPHGD=DBManager.getDBResultAsBeanList(sql, AccountableProductDataObject.class,Constants.EPH_URL);
    }


}