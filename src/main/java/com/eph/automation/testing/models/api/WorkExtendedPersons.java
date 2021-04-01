package com.eph.automation.testing.models.api;

import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import org.junit.Assert;

import java.util.HashMap;

public class WorkExtendedPersons {
    /**
     * created by Nishant @ 19 Jun 2020
     * for JRBI data reflect in APIv3 work extended, person extended and manifestation extended
     */


    private HashMap<String,Object> extendedRole;
    public HashMap<String, Object> getExtendedRole() {return extendedRole;}
    public void setExtendedRole(HashMap<String, Object> extendedRole) {this.extendedRole = extendedRole;}

    private HashMap<String, Object> extendedPerson;
    public HashMap<String, Object> getExtendedPerson() {return extendedPerson;}
    public void setExtendedPerson(HashMap<String, Object> extendedPerson) {this.extendedPerson = extendedPerson;}

    //added by Nishant @ 29 Jan 2021
    private String sequenceNumber;
    public String getSequenceNumber() {return sequenceNumber;}
    public void setSequenceNumber(String sequenceNumber) {this.sequenceNumber = sequenceNumber;}

    //added by Nishant @ 31 Mar 2021
    private String coreWorkPersonRoleId;
    public String getCoreWorkPersonRoleId() {return coreWorkPersonRoleId;}
    public void setCoreWorkPersonRoleId(String coreWorkPersonRoleId) {this.coreWorkPersonRoleId = coreWorkPersonRoleId;}

    public void compareWithDB()
    {
        Assert.assertEquals(extendedRole.get("code"), DataQualityContext.workExtendedTestClass.getWorkExtended().getWorkExtendedPersons());
//        printLog(extendedRole.get("code").toString());
    }

    private void printLog(String verified){Log.info("verified..."+verified);}
}
