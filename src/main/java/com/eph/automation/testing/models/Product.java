package com.eph.automation.testing.models;

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







}
