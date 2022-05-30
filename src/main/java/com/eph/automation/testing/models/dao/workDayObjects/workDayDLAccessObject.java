package com.eph.automation.testing.models.dao.workDayObjects;

public class workDayDLAccessObject {

    private String sourceref;
    private String given_name;
    private String family_name;
    private String peoplehub_id;
    private String email;
    private String end_date;


    public String getsourceref() {
        return sourceref;
    }
    public void setsourceref(String sourceref) {
        this.sourceref = sourceref;
    }

    public String getgiven_name() {
        return given_name;
    }
    public void setgiven_name(String given_name) {
        this.given_name = given_name;
    }

    public String getfamily_name() {
        return family_name;
    }
    public void setfamily_name(String family_name) {
        this.family_name = family_name;
    }

    public String getpeoplehub_id() {
        return peoplehub_id;
    }
    public void setpeoplehub_id(String peoplehub_id) {
        this.peoplehub_id = peoplehub_id;
    }

    public String getemail() {
        return email;
    }
    public void setemail(String email) {
        this.email = email;
    }

    public String getend_date() {
        return end_date;
    }
    public void setend_date(String end_date) {
        this.end_date = end_date;
    }


}

