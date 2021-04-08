package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 * updated by Nishant @ 30 Mar 2020 as per latest API changes
 */
import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.PersonDataObject;
import com.eph.automation.testing.models.dao.PersonWorkRoleDataObject;
import com.eph.automation.testing.models.dao.PersonProductRoleDataObject;
import com.eph.automation.testing.services.db.sql.PersonDataSQL;
import com.eph.automation.testing.services.db.sql.PersonWorkRoleDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;
import net.minidev.json.parser.ParseException;
import org.junit.Assert;

import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PersonsApiObject {
    @StaticInjection
    private DataQualityContext dataQualityContext;
    public PersonsApiObject() {}

    private String id;
    private HashMap<String, Object> role;
    private HashMap<String, String> person;
    private String effectiveStartDate;
    private String effectiveEndDate;

    public String getId() {return id;}
    public void setId(String id) {this.id = id;}

    public HashMap<String, Object> getRole() {return role;}
    public void setRole(HashMap<String, Object> role) {this.role = role;}

    public HashMap<String, String> getPerson() {return person;}
    public void setPerson(HashMap<String, String> person) {this.person = person;}

    public String getEffectiveStartDate(){return effectiveStartDate;}
    public void setEffectiveStartDate(String effectiveStartDate){this.effectiveStartDate=effectiveStartDate;}

    public String getEffectiveEndDate() {return effectiveEndDate;}
    public void setEffectiveEndDate(String effectiveEndDate) {this.effectiveEndDate = effectiveEndDate;}

    private void getPersonProductRoleRecordsEPHGD(String productPersonRoleID) {
        //created by Nishant @ 22 nov 2019
        String sql = String.format(PersonWorkRoleDataSQL.GET_DATA_PERSONS_PRODUCT_ROLE_EPHGD, productPersonRoleID);
        dataQualityContext.personProductRoleDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, PersonProductRoleDataObject.class, Constants.EPH_URL);
    }

    private void getPersonWorkRoleRecordsEPHGD(String workPersonRoleID) {
        String sql = String.format(PersonWorkRoleDataSQL.GET_DATA_PERSONS_WORK_ROLE_EPHGD, workPersonRoleID);
        dataQualityContext.personWorkRoleDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, PersonWorkRoleDataObject.class, Constants.EPH_URL);
    }

    public void getPersonDataFromEPHGD(String personID) {
        String sql = String.format(PersonDataSQL.GET_DATA_PERSONS_EPHGD, personID);
        dataQualityContext.personDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, PersonDataObject.class, Constants.EPH_URL);
    }

    public void compareWithDB_product() {
        //created by Nishant @ 22 Nov 2019
        //Updated by Nishant @ 30 Apr 2020
        Log.info("verifying product person... "+this.id);
        if(dataQualityContext.personDataObjectsFromEPHGD!=null){dataQualityContext.personDataObjectsFromEPHGD.clear();}
        if(dataQualityContext.personProductRoleDataObjectsFromEPHGD!=null){dataQualityContext.personProductRoleDataObjectsFromEPHGD.clear();}
        //Log.info("-person role\n-person id\n-persond first name\n-person family name\n-personhubId\n-person emailid");
        getPersonProductRoleRecordsEPHGD(this.id);
        Assert.assertEquals(dataQualityContext.personProductRoleDataObjectsFromEPHGD.get(0).getF_ROLE(), this.role.get("code"));
        printLog("person role");

        Assert.assertEquals(dataQualityContext.personProductRoleDataObjectsFromEPHGD.get(0).getF_PERSON(), String.valueOf(this.person.get("id")));
        printLog("person id");

        getPersonDataFromEPHGD( this.person.get("id").toString());
        Assert.assertEquals(dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FIRST_NAME(), this.person.get("firstName"));
        printLog("firstName");

        Assert.assertEquals(dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FAMILY_NAME(), this.person.get("lastName"));
        printLog("lastName");

        Assert.assertEquals(dataQualityContext.personDataObjectsFromEPHGD.get(0).getPEOPLEHUB_ID(),this.person.get("peoplehubId"));
        printLog("peoplehubId");

        Assert.assertEquals(dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_EMAIL_ID(),this.person.get("email"));
        printLog("email");
    }

    public void compareWithDB_work() {
        //Updated by Nishant @ 22 Nov 2019
        Log.info("verifiying work person... "+this.id);
        if(dataQualityContext.personDataObjectsFromEPHGD!=null){dataQualityContext.personDataObjectsFromEPHGD.clear();}
        if(dataQualityContext.personWorkRoleDataObjectsFromEPHGD!=null){dataQualityContext.personWorkRoleDataObjectsFromEPHGD.clear();}

        getPersonWorkRoleRecordsEPHGD(this.id);

        Assert.assertEquals(dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(0).getF_ROLE(), this.role.get("code"));
        printLog("person role");

        Assert.assertEquals(dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(0).getF_PERSON(), String.valueOf(this.person.get("id")));
        printLog("person id");

        getPersonDataFromEPHGD( this.person.get("id").toString());
        Assert.assertEquals(dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FIRST_NAME(), this.person.get("firstName"));
        printLog("firstName");

        Assert.assertEquals(dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FAMILY_NAME(), this.person.get("lastName"));
        printLog("lastName");

        if(!(dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_EMAIL_ID()==null&&this.person.get("email")==null)){
        Assert.assertEquals(dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_EMAIL_ID(),this.person.get("email"));
            printLog("email");}
       if(!(dataQualityContext.personDataObjectsFromEPHGD.get(0).getPEOPLEHUB_ID()==null&& this.person.get("peoplehubId")==null)){
        Assert.assertEquals(dataQualityContext.personDataObjectsFromEPHGD.get(0).getPEOPLEHUB_ID(),this.person.get("peoplehubId"));
           printLog("peoplehubId");}
    }
    private void printLog(String verified){Log.info("verified..."+verified);}

}