package com.eph.automation.testing.models.dao.JMDataLake.JsonObjects;

public class StitchSubIdentifierJson {
    private String identifier;
    public String getidentifier() {return identifier;}
    public void setidentifier(String identifier) {this.identifier = identifier;}

    private String leadInd;

    public String getleadInd() {
        return leadInd;
    }
    public void setleadInd(String leadInd){this.leadInd = leadInd;}

    private StitchSubWorkCoreJson id;
    public StitchSubWorkCoreJson getid() {return id;}
    public void setid(StitchSubWorkCoreJson id) {this.id = id;}

    private StitchSubWorkCoreJson identifierType;
    public StitchSubWorkCoreJson getidentifierType() {return identifierType;}
    public void setidentifierType(StitchSubWorkCoreJson identifierType) {this.identifierType = identifierType;}



    private StitchSubWorkCoreJson role;
    public StitchSubWorkCoreJson getrole() {return role;}
    public void setrole(StitchSubWorkCoreJson role) {this.role = role;}



    private StitchPersonJson [] workChild;
    public StitchPersonJson [] getworkChild() {return workChild;}
    public void setworkChild(StitchPersonJson [] workChild) {this.workChild = workChild;}

    private StitchPersonJson [] workParent;
    public StitchPersonJson [] getworkParent() {return workParent;}
    public void setworkParent(StitchPersonJson [] workParent) {this.workParent = workParent;}
 }
