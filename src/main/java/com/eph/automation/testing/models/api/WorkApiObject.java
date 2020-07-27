package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 * updated by Nishant @ 30 Mar 2020 as per latest API changes
 * moving all relevant tags inside newly added workCore object
 */

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.TestContext;
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

    public List<WorkDataObject> getWorkDataObjectsFromEPHGD() {
        return workDataObjectsFromEPHGD;
    }

    public void setWorkDataObjectsFromEPHGD(List<WorkDataObject> workDataObjectsFromEPHGD) {
        this.workDataObjectsFromEPHGD = workDataObjectsFromEPHGD;
    }

    private List<AccountableProductDataObject> accountableProductDataObjectsFromEPHGD;

    public List<AccountableProductDataObject> getAccountableProductDataObjectsFromEPHGD() {
        return accountableProductDataObjectsFromEPHGD;
    }

    public void setAccountableProductDataObjectsFromEPHGD(List<AccountableProductDataObject> accountableProductDataObjectsFromEPHGD) {
        this.accountableProductDataObjectsFromEPHGD = accountableProductDataObjectsFromEPHGD;
    }

    private WorkExtendedTestClass workExtendedTestClass;

    public WorkExtendedTestClass getWorkExtendedTestClass() {
        return workExtendedTestClass;
    }

    public void setWorkExtendedTestClass(WorkExtendedTestClass workExtendedTestClass) {
        this.workExtendedTestClass = workExtendedTestClass;
    }

    private String createdDate;

    public String getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(String createdDate) {
        this.createdDate = createdDate;
    }

    private String updatedDate;

    public String getUpdatedDate() {
        return updatedDate;
    }

    public void setUpdatedDate(String updatedDate) {
        this.updatedDate = updatedDate;
    }

    private String id;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    private workCore workCore;

    public workCore getWorkCore() {
        return workCore;
    }

    public void setWorkCore(workCore workCore) {
        this.workCore = workCore;
    }

    private WorkExtended workExtended;

    public WorkExtended getWorkExtended() {
        return workExtended;
    }

    public void setWorkExtended(WorkExtended workExtended) {
        this.workExtended = workExtended;
    }

    private WorkManifestationApiObject[] manifestations;

    public WorkManifestationApiObject[] getManifestations() {
        return manifestations;
    }

    public void setManifestations(WorkManifestationApiObject[] manifestations) {
        this.manifestations = manifestations;
    }

    //created by Nishant @ 17 Apr 2020
    private List<ManifestationProductAPIObject> products;

    public List<ManifestationProductAPIObject> getProducts() {
        return products;
    }

    public void setProducts(List<ManifestationProductAPIObject> products) {
        this.products = products;
    }

    public void compareWithDB() {//implemented by Nishant @ 23 Apr 2020
        //1
        workCore.compareWithDB(this.id);

        //2
        if (workExtended != null) {
            Log.info("\nVerifying workExtended..." + this.id);
            getJsonToObject_extendedWork(this.id);
            workExtended.compareWithDB();
        }

        //3
        Log.info("\nVerifying work manifestations...");
        Log.info("total Manifestations found..."+manifestations.length);
        for (WorkManifestationApiObject manifestation : manifestations) {
            manifestation.compareWithDB();
        }

        //4
        if (products != null) {
            Log.info("\nVerifying work products...");
            Log.info("total work products found..."+products.size());
            for (ManifestationProductAPIObject Workproducts : products) {
                Workproducts.compareWithDB();
            }
        }
    }

    public void getJsonToObject_extendedWork(String workId) {
        //created by Nishant @ 19 Jun 2020 to verify extended json value with APIv3
        //updated by Nishant @ 08 Jul 2020 for JRBI data validation on UAT JF UI
        String sql ="";
        if(TestContext.getValues().environment=="UAT")
             sql = "SELECT \"json\" FROM ephuat_extended_data_stitch.stch_work_ext_json where epr_id='" + workId + "'";
        else sql = "SELECT \"json\" FROM ephsit_extended_data_stitch.stch_work_ext_json where epr_id='" + workId + "'";


        List<Map<String, String>> jsonValue = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        DataQualityContext.workExtendedTestClass = new Gson().fromJson(jsonValue.get(0).get("json"), WorkExtendedTestClass.class);
    }

}