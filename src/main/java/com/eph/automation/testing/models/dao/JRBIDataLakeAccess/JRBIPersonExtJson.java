package com.eph.automation.testing.models.dao.JRBIDataLakeAccess;
//package com.eph.automation.testing.models.api;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.contexts.JRBIAccessDLContext;
import org.junit.Assert;
import java.util.HashMap;
import com.eph.automation.testing.models.dao.JRBIDataLakeAccess.JRBIWorkExtJson;

public class JRBIPersonExtJson {

private HashMap<String,Object> extendedRole;
public HashMap<String,Object>getExtendedRole() {return extendedRole;}
public void setExtendedRole(HashMap<String,Object> extendedRole) {this.extendedRole = extendedRole;}

private HashMap<String,Object> extendedPerson;
public HashMap<String,Object> getExtendedPerson() {return extendedPerson;}
public void setExtendedPerson(HashMap<String,Object> extendedPerson) {this.extendedPerson= extendedPerson;}

/*
public void compareWithDB()
{
Assert.assertEquals(extendedRole.get("code"), JRBIAccessDLContext.recordsFromExtendeWork);
printLog(extendedRole.get("code").toString());
}

private void printLog(String verified){Log.info("verified..."+verified);}
*/

}

