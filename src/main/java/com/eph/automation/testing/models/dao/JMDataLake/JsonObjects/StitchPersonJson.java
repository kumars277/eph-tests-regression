package com.eph.automation.testing.models.dao.JMDataLake.JsonObjects;


public class StitchPersonJson {
    private String id;
    public String getid() {return id;}
    public void setid(String id) {this.id = id;}

    private StitchSubWorkCoreJson person;
    public StitchSubWorkCoreJson getperson() {return person;}
    public void setperson(StitchSubWorkCoreJson person) {this.person = person;}

    private StitchSubWorkCoreJson subjectArea;
    public StitchSubWorkCoreJson getsubjectArea() {return subjectArea;}
    public void setsubjectArea(StitchSubWorkCoreJson subjectArea) {this.subjectArea = subjectArea;}

    private StitchSubWorkCoreJson role;
    public StitchSubWorkCoreJson getrole() {return role;}
    public void setrole(StitchSubWorkCoreJson role) {this.role = role;}
}
