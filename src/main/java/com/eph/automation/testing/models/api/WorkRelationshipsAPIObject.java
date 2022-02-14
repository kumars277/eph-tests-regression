package com.eph.automation.testing.models.api;

/*
 * Created by Nishant @ 5 May 2020
 * */

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.eph.automation.testing.services.db.sql.WorkRelationshipDataObject;
import com.eph.automation.testing.services.db.sql.WorkRelationshipSQL;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;
import org.junit.Assert;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.eph.automation.testing.models.contexts.DataQualityContext.getBreadcrumbMessage;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkRelationshipsAPIObject {

  public WorkRelationshipsAPIObject() {}

  private DataQualityContext dataQualityContext;

  private workParent[] workParent;

  public WorkRelationshipsAPIObject.workParent[] getWorkParent() {
    return workParent;
  }

  public void setWorkParent(WorkRelationshipsAPIObject.workParent[] workParent) {
    this.workParent = workParent;
  }

  private workChild[] workChild;

  public WorkRelationshipsAPIObject.workChild[] getWorkChild() {
    return workChild;
  }

  public void setWorkChild(WorkRelationshipsAPIObject.workChild[] workChild) {
    this.workChild = workChild;
  }

  public void compareWithDB(String workId) throws ParseException {
    // updated by Nishant @ 05 Apr 2021
    Log.info("verifying workRelationship..." + workId);
    if (workParent != null) {
      ArrayList<workParent> list_workParent = new ArrayList<>(Arrays.asList(workParent));
      getWorkRelationshipParentRecordsEPHGD(workId);
      for (int wp = 0; wp < workParent.length; wp++) {
          if(list_workParent.get(wp).workSummary.status.get("code").toString().equalsIgnoreCase("NVW"))
          {continue;}

        boolean parentFound = false;
        for (int wp2 = 0; wp2 < workParent.length; wp2++) {
          if (list_workParent.get(wp).id.equalsIgnoreCase(dataQualityContext.workRelationshipParentDataObjectsFromEPGD.get(wp2).getF_PARENT()) &&
                  list_workParent.get(wp).type.get("code").toString().equalsIgnoreCase(dataQualityContext.workRelationshipParentDataObjectsFromEPGD.get(wp2).getF_RELATIONSHIP_TYPE()))
           {
             parentFound = true;
            Log.info("verifying workParent " + list_workParent.get(wp).id+", code "+list_workParent.get(wp).type.get("code"));

            Assert.assertEquals(workId + " - parent effectiveStartDate",list_workParent.get(wp).effectiveStartDate,dataQualityContext.workRelationshipParentDataObjectsFromEPGD.get(wp2).getEFFECTIVE_START_DATE());
            printLog("effectiveStartDate");

            getWorksDataFromEPHGD(list_workParent.get(wp).id);

            Assert.assertEquals(workId + " - parent work Title",list_workParent.get(wp).workSummary.title,dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
            printLog("work Title");

            Assert.assertEquals(workId + " - parent work Type",list_workParent.get(wp).workSummary.type.get("code"),dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TYPE());
            printLog("work Type");

            Assert.assertEquals(workId + " -parent work Status",list_workParent.get(wp).workSummary.status.get("code"),dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_STATUS());
            printLog("work Status");
            break;
          }
        }
        Assert.assertTrue(getBreadcrumbMessage()+" Parent not found, "+list_workParent.get(wp).id+" parent code "+list_workParent.get(wp).type.get("code"), parentFound);
      }
    }
    if (workChild != null) {
      ArrayList<workChild> list_workChild = new ArrayList<>(Arrays.asList(workChild));
      getWorkRelationshipChildRecordsEPHGD(workId);
      for (int wc = 0; wc < workChild.length; wc++) {
          //DateFormat format = new SimpleDateFormat("MMMM d, yyyy", Locale.ENGLISH);
          //Date date = format.parse(list_workChild.get(wc).effectiveEndDate);
          if(isExpired(list_workChild.get(wc).effectiveEndDate)) continue;
        boolean childFound = false;
        for (int wc2 = 0; wc2 < workChild.length; wc2++) {
          if (list_workChild.get(wc).id.equalsIgnoreCase(dataQualityContext.workRelationshipChildDataObjectsFromEPGD.get(wc2).getF_CHILD()) &&
                  list_workChild.get(wc).type.get("code").toString().equalsIgnoreCase(dataQualityContext.workRelationshipChildDataObjectsFromEPGD.get(wc2).getF_RELATIONSHIP_TYPE()))
           {
             childFound=true;
            Log.info("verifying workChild " + list_workChild.get(wc).id+", child "+list_workChild.get(wc).type.get("code"));

            Assert.assertEquals(workId + " - child effectiveStartDate",list_workChild.get(wc).effectiveStartDate,dataQualityContext.workRelationshipChildDataObjectsFromEPGD.get(wc2).getEFFECTIVE_START_DATE());
            printLog("effectiveStartDate");

            getWorksDataFromEPHGD(list_workChild.get(wc).id);

            Assert.assertEquals(workId + " - child work Title",list_workChild.get(wc).workSummary.title,dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
            printLog("work Title");

            Assert.assertEquals(workId + " - child work Type",list_workChild.get(wc).workSummary.type.get("code"),dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TYPE());
            printLog("work Type");

            Assert.assertEquals(workId + " - child work Status",list_workChild.get(wc).workSummary.status.get("code"),dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_STATUS());
            printLog("work Status");

            if (list_workChild.get(wc).effectiveEndDate != null) {Assert.assertEquals(workId + " - child effectiveStartDate",list_workChild.get(wc).effectiveEndDate,
                    dataQualityContext.workRelationshipChildDataObjectsFromEPGD.get(wc2).getEFFECTIVE_END_DATE());
              printLog("effectiveEndDate");
            }
          }
        }
        Assert.assertTrue(getBreadcrumbMessage()+" Child not found, "+list_workChild.get(wc).id+" child code "+list_workChild.get(wc).type.get("code"), childFound);
      }
    }
  }

  public boolean isExpired(String date) throws ParseException {
      Boolean endDated = false;
      SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd",Locale.ENGLISH);
    if(date!=null){
      Date DateToCmp = dateFormatter.parse(date);
      Date Todate = new Date();
      if(Todate.compareTo(DateToCmp)>0){    endDated = true;}
    }
        return endDated;
  }

  private void getWorkRelationshipParentRecordsEPHGD(String workId) {
    Log.info("getParent record of..." + workId);
    String sql = String.format(WorkRelationshipSQL.getWorkParent, workId);
    dataQualityContext.workRelationshipParentDataObjectsFromEPGD =
        DBManager.getDBResultAsBeanList(sql, WorkRelationshipDataObject.class, Constants.EPH_URL);
  }

  private void getWorkRelationshipChildRecordsEPHGD(String workId) {
    Log.info("getChild record of..." + workId);
    String sql = String.format(WorkRelationshipSQL.getWorkChild, workId);
    dataQualityContext.workRelationshipChildDataObjectsFromEPGD =
        DBManager.getDBResultAsBeanList(sql, WorkRelationshipDataObject.class, Constants.EPH_URL);
  }

  private void getWorksDataFromEPHGD(String workId) {
    Log.info("getWork record ..." + workId);
    String sql = String.format(APIDataSQL.GET_GD_DATA_WORK, workId);
    dataQualityContext.workDataObjectsFromEPHGD =
        DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    dataQualityContext.workDataObjectsFromEPHGD.sort(
        Comparator.comparing(WorkDataObject::getWORK_ID));
    Assert.assertFalse(
        "Verify that list with work objects from DB is not empty",
        dataQualityContext.workDataObjectsFromEPHGD.isEmpty());
  }

  public static class workParent {
    private HashMap<String, Object> type;

    public HashMap<String, Object> getType() {
      return type;
    }

    public void setType(HashMap<String, Object> type) {
      this.type = type;
    }

    private String id;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }
/*
    private workSummary workSummary;

    public WorkRelationshipsAPIObject.workSummary getWorkSummary() {
      return workSummary;
    }

    public void setWorkSummary(WorkRelationshipsAPIObject.workSummary workSummary) {
      this.workSummary = workSummary;
    }
*/
    private WorkSummary workSummary;
    public WorkSummary getWorkSummary() {return workSummary;}
    public void setWorkSummary(WorkSummary workSummary) {this.workSummary = workSummary;}

    private String effectiveStartDate;

    public String getEffectiveStartDate() {
      return effectiveStartDate;
    }

    public void setEffectiveStartDate(String effectiveStartDate) {
      this.effectiveStartDate = effectiveStartDate;
    }

    private String effectiveEndDate;

    public String getEffectiveEndDate() {
      return effectiveEndDate;
    }

    public void setEffectiveEndDate(String effectiveEndDate) {
      this.effectiveEndDate = effectiveEndDate;
    }
  }

  public static class workChild {
    private HashMap<String, Object> type;

    public HashMap<String, Object> getType() {
      return type;
    }

    public void setType(HashMap<String, Object> type) {
      this.type = type;
    }

    private String id;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }
/*
    private workSummary workSummary;
    public WorkRelationshipsAPIObject.workSummary getWorkSummary() {
      return workSummary;
    }
    public void setWorkSummary(WorkRelationshipsAPIObject.workSummary workSummary) {
      this.workSummary = workSummary;
    }
*/
    private WorkSummary workSummary;
    public WorkSummary getWorkSummary() {return workSummary;}
    public void setWorkSummary(WorkSummary workSummary) {this.workSummary = workSummary;}

    private String effectiveStartDate;
    public String getEffectiveStartDate() {return effectiveStartDate;}
    public void setEffectiveStartDate(String effectiveStartDate) {
      this.effectiveStartDate = effectiveStartDate;
    }

    private String effectiveEndDate;
    public String getEffectiveEndDate() {
      return effectiveEndDate;
    }
    public void setEffectiveEndDate(String effectiveEndDate) {
      this.effectiveEndDate = effectiveEndDate;
    }
  }
/*
  public static class workSummary {
    private String title;
    public void setTitle(String title) {this.title = title;}
    public String getTitle() {return title;}

    private HashMap<String, Object> type;
    public HashMap<String, Object> getType() {return type;}
    public void setType(HashMap<String, Object> type) {this.type = type;}

    private HashMap<String, Object> status;
    public void setStatus(HashMap<String, Object> status) {this.status = status;}
    public HashMap<String, Object> getStatus() {return status;}
  }*/

  private void printLog(String verified) {Log.info("verified..." + verified);}
}
