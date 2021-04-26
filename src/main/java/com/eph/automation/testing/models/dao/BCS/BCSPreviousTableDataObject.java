//created by Nishant @ 26 Nov 2020
package com.eph.automation.testing.models.dao.BCS;

public class BCSPreviousTableDataObject {
    private String businessunit;
    private String classificationcode;
    private String classificationtype;
    private String metadeleted;
    private String metamodifiedon;
    private String priority;
    private String sourceref;
    private String value;

    public String getMetadeleted() {return metadeleted;}
    public void setMetadeleted(String metadeleted) {this.metadeleted = metadeleted;}

    public String getMetamodifiedon() {return metamodifiedon;}
    public void setMetamodifiedon(String metamodifiedon) {this.metamodifiedon = metamodifiedon;}

    public String getSourceref() {return sourceref;}
    public void setSourceref(String sourceref) {this.sourceref = sourceref;}

    public String getClassificationtype() {return classificationtype;}
    public void setClassificationtype(String classificationtype) {this.classificationtype = classificationtype;}

    public String getPriority() {return priority;}
    public void setPriority(String priority) {this.priority = priority;}

    public String getValue() {return value;}
    public void setValue(String value) {this.value = value;}

    public String getBusinessunit() {return businessunit;}
    public void setBusinessunit(String businessunit) {this.businessunit = businessunit;}

    public String getClassificationcode() {return classificationcode;}
    public void setClassificationcode(String classificationcode) {this.classificationcode = classificationcode;}

}
