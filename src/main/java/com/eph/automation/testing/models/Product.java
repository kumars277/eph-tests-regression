package com.eph.automation.testing.models;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.WorkExtractSQL;

import java.util.List;

/**
 * Created by RAVIVARMANS on 28/11/2018.
 */
public class Product {

    public String name;
    public String shortName;

    public String productIdentifier;

    public String productPersonRole;

    public String productManifestationKeyTitle;

    public String productManifestationIdentifier;

    //Product Line
    public String productLineId;
    public String productLineTypeName;
    public String productLineParentId;

    //Product Work
    public String workID;
    public String workTitle;
    public String workSubTitle;
    public String fType;
    public String fStatus;
    public String workKeyTitle;
    public String electroRightsIndicator;
    public String volume;
    public String copyRightYear;
    public String fAccountableProduct;
    public String fPMC;
    public String fOAType;
    public String fFmily;
    public String fImPrint;
    public String fsocietyOwnership;
    public String fAccessModel;



    public static Product getNewProduct() {
        Product product = new Product();
        product.productLineId = "1000000";
        product.productLineTypeName = "A";
        product.productLineParentId = "HS";
        return product;
    }

     public static Product getNewProductb() {
        Product product = new Product();
        product.productLineId = "1000000";
        product.productLineTypeName = "A";
        product.productLineParentId = "HS";
        return product;
    }


    public static Product getNewProductWork() {
        Product productWork = new Product();

        return productWork;
    }


    public static Product getNewProductFromPMX(String pmc) {
        Product productWork = new Product();
        String SQL = WorkExtractSQL.GET_PRODUCT_EXPORT_FROM_PMX_BY_PMC.replace("PARAM1",pmc);
        List<WorkDataObject> productWorkFromPMX = DBManager.getDBResultAsBeanList(SQL,
                WorkDataObject.class, Constants.PMX_URL);
        WorkDataObject productFromPMX = productWorkFromPMX.get(0);
        productWork.workID = productFromPMX.PRODUCT_WORK_ID;
        productWork.workTitle = productFromPMX.WORK_TITLE;
        return productWork;
    }
}
