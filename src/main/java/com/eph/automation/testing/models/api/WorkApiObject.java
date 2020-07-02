package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 * updated by Nishant @ 30 Mar 2020 as per latest API changes
 * moving all relevant tags inside newly added workCore object
 */
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.AccountableProductDataObject;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.eph.automation.testing.services.db.sql.DataQualityChecksSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import org.junit.Assert;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkApiObject {

    private List<WorkDataObject> workDataObjectsFromEPHGD;
    public List<WorkDataObject> getWorkDataObjectsFromEPHGD() {return workDataObjectsFromEPHGD;}
    public void setWorkDataObjectsFromEPHGD(List<WorkDataObject> workDataObjectsFromEPHGD) {this.workDataObjectsFromEPHGD = workDataObjectsFromEPHGD;}

    private List<AccountableProductDataObject> accountableProductDataObjectsFromEPHGD;
    public List<AccountableProductDataObject> getAccountableProductDataObjectsFromEPHGD() {return accountableProductDataObjectsFromEPHGD;}
    public void setAccountableProductDataObjectsFromEPHGD(List<AccountableProductDataObject> accountableProductDataObjectsFromEPHGD) {this.accountableProductDataObjectsFromEPHGD = accountableProductDataObjectsFromEPHGD;}

    private WorkExtendedTestClass workExtendedTestClass;
    public WorkExtendedTestClass getWorkExtendedTestClass() {return workExtendedTestClass;}
    public void setWorkExtendedTestClass(WorkExtendedTestClass workExtendedTestClass) {this.workExtendedTestClass = workExtendedTestClass;}

    private String createdDate;
    public String getCreatedDate(){return  createdDate;}
    public void setCreatedDate(String createdDate) {this.createdDate = createdDate;}

    private String updatedDate;
    public String getUpdatedDate(){return updatedDate;}
    public void setUpdatedDate(String updatedDate) {this.updatedDate = updatedDate;}

    private String id;
    public String getId() {return id;}
    public void setId(String id) {this.id = id;}

    private workCore workCore;
    public workCore getWorkCore() {return workCore;}
    public void setWorkCore(workCore workCore) {this.workCore = workCore;}

    private WorkExtended workExtended;
    public WorkExtended getWorkExtended() {return workExtended;}
    public void setWorkExtended(WorkExtended workExtended) {this.workExtended = workExtended;}

    private WorkManifestationApiObject[] manifestations;
    public WorkManifestationApiObject[] getManifestations() {return manifestations;}
    public void setManifestations(WorkManifestationApiObject[] manifestations) {this.manifestations = manifestations;}

    //created by Nishant @ 17 Apr 2020
    private List<ManifestationProductAPIObject> products;
    public List<ManifestationProductAPIObject> getProducts() {return products;}
    public void setProducts(List<ManifestationProductAPIObject> products) {this.products = products;}

    public void compareWithDB(WorkDataObject workId){//implemented by Nishant @ 23 Apr 2020

  //   workCore.compareWithDB(this.id);

     for (WorkManifestationApiObject manifestation : manifestations){manifestation.compareWithDB(workId);}

    //if(products!=null) {Log.info("verifying work products...");for (ManifestationProductAPIObject Workproducts : products) {Workproducts.compareWithDB();}}

   //  if(workExtended!=null){Log.info("verifying work Extended..."+this.id);getJsonToObject_extendedWork(this.id);workExtended.compareWithDB();}

    }

    public void getJsonToObject_extendedWork(String workId)
    {//created by Nishant @ 19 Jun 2020 to verify extended json value with APIv3
        String sql="SELECT \"json\" FROM ephsit_extended_data_stitch.stch_work_ext_json where epr_id='"+workId+"'";
        List<Map<String,String>> jsonValue=DBManager.getDBResultMap(sql,Constants.EPH_URL);
        DataQualityContext.workExtendedTestClass = new Gson().fromJson(jsonValue.get(0).get("json"), WorkExtendedTestClass.class);
    }



}