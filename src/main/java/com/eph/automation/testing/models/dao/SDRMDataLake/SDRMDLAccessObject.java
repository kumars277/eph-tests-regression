package com.eph.automation.testing.models.dao.SDRMDataLake;

public class SDRMDLAccessObject {

    private String isbn;
    private String title;
    private String rednitionFormat;
    private String inboundTs;
    private String productionDate;
    private String eprId;
    private String productType;
    private String uKey;
    private String deltaMode;

    public String getuKey() {
        return uKey;
    }
    public void setuKey(String uKey) {
        this.uKey = uKey;
    }

    public String getdeltaMode() {
        return deltaMode;
    }
    public void setdeltaMode(String deltaMode) {
        this.deltaMode = deltaMode;
    }


    public String getisbn() {
        return isbn;
    }
    public void setisbn(String isbn) {
        this.isbn = isbn;
    }

    public String gettitle() {
        return title;
    }
    public void settitle(String title) {
        this.title = title;
    }

    public String getrednitionFormat() {
        return rednitionFormat;
    }
    public void setrednitionFormat(String rednitionFormat) {
        this.rednitionFormat = rednitionFormat;
    }

    public String getinboundTs() {
        return inboundTs;
    }
    public void setinboundTs(String inboundTs) {
        this.inboundTs = inboundTs;
    }

    public String getproductionDate() {
        return productionDate;
    }
    public void setproductionDate(String productionDate) {
        this.productionDate = productionDate;
    }

    public String geteprId() {
        return eprId;
    }
    public void seteprId(String eprId) {
        this.eprId = eprId;
    }

    public String getproductType() {
        return productType;
    }
    public void setproductType(String productType) {
        this.productType = productType;
    }



}
