package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 */
import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.PersonDataObject;
import com.eph.automation.testing.models.dao.PersonWorkRoleDataObject;
import com.eph.automation.testing.services.db.sql.PersonDataSQL;
import com.eph.automation.testing.services.db.sql.PersonWorkRoleDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;
import org.junit.Assert;

import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
class PersonsApiObject {
    @StaticInjection
    private DataQualityContext dataQualityContext;
    public PersonsApiObject() {
    }

    public void compareWithDB(){
        if(dataQualityContext.personDataObjectsFromEPHGD!=null)
        {dataQualityContext.personDataObjectsFromEPHGD.clear();}
        if(dataQualityContext.personWorkRoleDataObjectsFromEPHGD!=null)
        {dataQualityContext.personWorkRoleDataObjectsFromEPHGD.clear();}

        getPersonWorkRoleRecordsEPHGD(this.id);
        Assert.assertEquals(dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(0).getF_ROLE(), this.role.get("code"));
        Assert.assertEquals(dataQualityContext.personWorkRoleDataObjectsFromEPHGD.get(0).getF_PERSON(), String.valueOf(this.person.get("id")));
        getPersonDataFromEPHGD( this.person.get("id").toString());
        Assert.assertEquals(dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FIRST_NAME(), this.person.get("firstName"));
        Assert.assertEquals(dataQualityContext.personDataObjectsFromEPHGD.get(0).getPERSON_FAMILY_NAME(), this.person.get("lastName"));

    }
    private String id;
    private HashMap<String, Object> role;
    private HashMap<String, Object> person;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public HashMap<String, Object> getRole() {
        return role;
    }

    public void setRole(HashMap<String, Object> role) {
        this.role = role;
    }

    public HashMap<String, Object> getPerson() {
        return person;
    }

    public void setPerson(HashMap<String, Object> person) {
        this.person = person;
    }

    public void getPersonWorkRoleRecordsEPHGD(String workPersonRoleID) {
        Log.info("Get the person work role records from EPH GD  ..");
        String sql = String.format(PersonWorkRoleDataSQL.GET_DATA_PERSONS_WORK_ROLE_EPHGD, workPersonRoleID);
        Log.info(sql);
        dataQualityContext.personWorkRoleDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, PersonWorkRoleDataObject.class, Constants.EPH_URL);
    }

    public void getPersonDataFromEPHGD(String personID) {
        Log.info("Get Person Data From GD");
        String sql = String.format(PersonDataSQL.GET_DATA_PERSONS_EPHGD, personID);
        Log.info(sql);

        dataQualityContext.personDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, PersonDataObject.class, Constants.EPH_URL);
    }
}