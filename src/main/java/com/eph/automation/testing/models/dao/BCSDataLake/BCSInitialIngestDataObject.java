//created by Nishant @ 30 Sep 2020
package com.eph.automation.testing.models.dao.BCSDataLake;

public class BCSInitialIngestDataObject {
    private String metadeleted;
    private String metamodifiedon;
    private String sourceref;
    private String classificationcode;
    private String clvalue;
    private String classificationtype;
    private String priority;
    private String businessunit;

    public String getClassificationcode() {return classificationcode;}
    public void setClassificationcode(String classificationcode) {this.classificationcode = classificationcode;}

    public String getClassificationtype() {return classificationtype;}
    public void setClassificationtype(String classificationtype) {this.classificationtype = classificationtype;}

    public String getMetadeleted() {return metadeleted;}
    public void setMetadeleted(String metadeleted) {this.metadeleted = metadeleted;}

    public String getMetamodifiedon() {return metamodifiedon;}
    public void setMetamodifiedon(String metamodifiedon) {this.metamodifiedon = metamodifiedon;}

    public String getBusinessunit() {return businessunit;}
    public void setBusinessunit(String businessunit) {this.businessunit = businessunit;}

    public String getPriority() {return priority;}
    public void setPriority(String priority) {this.priority = priority;}

    public String getSourceref() {return sourceref;}
    public void setSourceref(String sourceref) {this.sourceref = sourceref;}

    public String getClvalue() {return clvalue;}
    public void setClvalue(String value) {this.clvalue = value;}
}
