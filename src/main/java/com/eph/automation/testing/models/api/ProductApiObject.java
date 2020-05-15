package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 * updated by Nishant @ 28 Apr 2020
 * optimised by Nishant @ 13 May 2020
 */
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.dao.ProductDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;
import org.junit.Assert;
import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ProductApiObject {
    public ProductApiObject() {}
    private static List<ProductDataObject> productDataObjectsFromEPHGD;

    public String createdDate;
    public String getCreatedDate() {return createdDate;}
    public void setCreatedDate(String createdDate) {this.createdDate = createdDate;}

    public String updatedDate;
    public String getUpdatedDate() {return updatedDate;}
    public void setUpdatedDate(String updatedDate) {this.updatedDate = updatedDate;}

    private String id;
    public String getId() {return id;}
    public void setId(String id) {this.id = id;}

    private productCore productCore;
    public productCore getProductCore() {return productCore;}
    public void setProductCore(productCore productCore) {this.productCore = productCore;}

    public ProductManifestationApiObject manifestation;
    public ProductManifestationApiObject getManifestationApiObject() {return manifestation;}
    public void setManifestationApiObject(ProductManifestationApiObject manifestation) {this.manifestation = manifestation;}

    private work work;
    public ProductApiObject.work getWork() {return work;}
    public void setWork(ProductApiObject.work work) {this.work = work;}

    public class work{
        private String id;
        public String getId() {return id;}
        public void setId(String id) {this.id = id;}

        private workCore workCore;
        public workCore getWorkCore() {return workCore;}
        public void setWorkCore(workCore workCore) {this.workCore = workCore;}
        }

    public void compareWithDB(){
        getProductDataFromEPHGD(this.id);
        createdDate=createdDate.replace("T"," ").replace("Z","");
        updatedDate=updatedDate.replace("T"," ").replace("Z","");
        Assert.assertEquals(createdDate,this.productDataObjectsFromEPHGD.get(0).getCREATED());
    //  Assert.assertEquals(updatedDate,this.productDataObjectsFromEPHGD.get(0).getUPDATED());

        productCore.compareWithDB(this.id);

        if(manifestation!=null) {manifestation.compareWithDB();}

        //implemented by Nishant @ 8 May 2020
        if(work!=null){work.workCore.compareWithDB(work.id);}   //compare product work - EPR-111MK1

    }

    private void getProductDataFromEPHGD(String workID) {
        List<String> ids = new ArrayList<>();
        ids.add(workID);
        String sql = String.format(APIDataSQL.EPH_GD_PRODUCT_EXTRACT_FOR_SEARCH, Joiner.on("','").join(ids));
        productDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
    }

}