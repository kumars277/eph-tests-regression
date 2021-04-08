package com.eph.automation.testing.models.api;
//created by Nishant @ 29 Jan 2021, EPHD-2747
public class PricingExtendedPricesAPIObj {

    private String currency;
    public String getCurrency() {return currency;}
    public void setCurrency(String currency) {this.currency = currency;}

    private String amount;
    public String getAmount() {return amount;}
    public void setAmount(String amount) {this.amount = amount;}

    private String startDate;
    public String getStartDate() {return startDate;}
    public void setStartDate(String startDate) {this.startDate = startDate;}

    private String endDate;
    public String getEndDate() {return endDate;}
    public void setEndDate(String endDate) {this.endDate = endDate;}

    private String region;
    public String getRegion() {return region;}
    public void setRegion(String region) {this.region = region;}

    private String customerCategory;
    public String getCustomerCategory() {return customerCategory;}
    public void setCustomerCategory(String customerCategory) {this.customerCategory = customerCategory;}

    //new tag - created by Nishant @ 05 Apr 2021
    private String category;
    public String getCategory() {return category;}
    public void setCategory(String category) {this.category = category;}

    private String purchaseQuantity;
    public String getPurchaseQuantity() {return purchaseQuantity;}
    public void setPurchaseQuantity(String purchaseQuantity) {this.purchaseQuantity = purchaseQuantity;}
}
