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

    private workCore workCore;
    public workCore getWorkCore() {return workCore;}
    public void setWorkCore(workCore workCore) {this.workCore = workCore;}

    private WorkManifestationApiObject[] manifestations;
    public WorkManifestationApiObject[] getManifestations() {return manifestations;}
    public void setManifestations(WorkManifestationApiObject[] manifestations) {this.manifestations = manifestations;}

    //created by Nishant @ 17 Apr 2020
    private List<ManifestationProductAPIObject> products;
    public List<ManifestationProductAPIObject> getProducts() {return products;}
    public void setProducts(List<ManifestationProductAPIObject> products) {this.products = products;}

    public void compareWithDB(){
        workCore.compareWithDB(this.id);

        for (WorkManifestationApiObject manifestation : manifestations){manifestation.compareWithDB();}

        //implemented by Nishant @ 23 Apr 2020
      if(products!=null) {Log.info("verifying work products...");
          for (ManifestationProductAPIObject Workproducts : products) {Workproducts.compareWithDB();}
      }
    }

}