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
import com.eph.automation.testing.steps.GenericFunctions;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.Assert;

import java.text.ParseException;
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
      try{
        Log.info("verifying workRelationship..." + workId);
        if (workParent != null) {
            ArrayList<workParent> list_workParent = new ArrayList<>(Arrays.asList(workParent));
            getWorkRelationshipParentRecordsEPHGD(workId);
            for (int wp = 0; wp < workParent.length; wp++) {
                Log.info("verifying parent "+list_workParent.get(wp).id);
                if(list_workParent.get(wp).workSummary.status.get("code").toString().equalsIgnoreCase("NVW"))
                {continue;}
                if(GenericFunctions.isExpired(list_workParent.get(wp).effectiveEndDate)) continue;
                boolean parentFound = false;
                for (int wp2 = 0; wp2 < dataQualityContext.workRelationshipParentDataObjectsFromEPGD.size(); wp2++) {
                    if (list_workParent.get(wp).id.equalsIgnoreCase(dataQualityContext.workRelationshipParentDataObjectsFromEPGD.get(wp2).getF_PARENT())
                            && list_workParent.get(wp).type.get("code").toString().equalsIgnoreCase(dataQualityContext.workRelationshipParentDataObjectsFromEPGD.get(wp2).getF_RELATIONSHIP_TYPE())
                    //&& list_workParent.get(wp).effectiveStartDate.toString().equalsIgnoreCase(dataQualityContext.workRelationshipParentDataObjectsFromEPGD.get(wp2).getEFFECTIVE_START_DATE())
                    )
                    {
                        parentFound = true;
                        Log.info("verifying workParent " + list_workParent.get(wp).id+", " +
                                "code "+list_workParent.get(wp).type.get("code")+
                                ", effectiveStartDate "+list_workParent.get(wp).effectiveStartDate );

                        if (list_workParent.get(wp).effectiveEndDate != null)
                        {Assert.assertEquals(getBreadcrumbMessage() + " - parent effectiveEndDate",list_workParent.get(wp).effectiveEndDate,
                                dataQualityContext.workRelationshipParentDataObjectsFromEPGD.get(wp2).getEFFECTIVE_END_DATE());
                            printLog("effectiveEndDate");}

                        getWorksDataFromEPHGD(list_workParent.get(wp).id);

                        Assert.assertEquals(getBreadcrumbMessage()+ " - parent work Title",list_workParent.get(wp).workSummary.title,
                                dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
                        printLog("work Title");

                        Assert.assertEquals(getBreadcrumbMessage()+ " - parent work Type",list_workParent.get(wp).workSummary.type.get("code"),dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TYPE());
                        printLog("work Type");

                        Assert.assertEquals(getBreadcrumbMessage()+ " -parent work Status",list_workParent.get(wp).workSummary.status.get("code"),dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_STATUS());
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
                if(GenericFunctions.isExpired(list_workChild.get(wc).effectiveEndDate)) continue;
                boolean childFound = false;
                for (int wc2 = 0; wc2 < workChild.length; wc2++) {
                    if (list_workChild.get(wc).id.equalsIgnoreCase(dataQualityContext.workRelationshipChildDataObjectsFromEPGD.get(wc2).getF_CHILD())
                            && list_workChild.get(wc).type.get("code").toString().equalsIgnoreCase(dataQualityContext.workRelationshipChildDataObjectsFromEPGD.get(wc2).getF_RELATIONSHIP_TYPE())
                    //&& list_workChild.get(wc).effectiveStartDate.equalsIgnoreCase(dataQualityContext.workRelationshipChildDataObjectsFromEPGD.get(wc2).getEFFECTIVE_START_DATE())
                    )
                    {
                        childFound=true;
                        Log.info("verifying workChild " + list_workChild.get(wc).id
                                + ", code "+ list_workChild.get(wc).type.get("code")
                                //+ ", effectiveStartDate "+list_workChild.get(wc).effectiveStartDate
                        );

                        if (list_workChild.get(wc).effectiveEndDate != null) {Assert.assertEquals(workId + " - child effectiveEndDate",list_workChild.get(wc).effectiveEndDate,
                                dataQualityContext.workRelationshipChildDataObjectsFromEPGD.get(wc2).getEFFECTIVE_END_DATE());
                            printLog("effectiveEndDate");}

                        getWorksDataFromEPHGD(list_workChild.get(wc).id);

                        Assert.assertEquals(getBreadcrumbMessage()+workId + " - child work Title",list_workChild.get(wc).workSummary.title,dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
                        printLog("work Title");

                        Assert.assertEquals(getBreadcrumbMessage()+workId + " - child work Type",list_workChild.get(wc).workSummary.type.get("code"),dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TYPE());
                        printLog("work Type");

                        Assert.assertEquals(getBreadcrumbMessage()+workId + " - child work Status",list_workChild.get(wc).workSummary.status.get("code"),dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_STATUS());
                        printLog("work Status");
                        break;
                    }
                }
                Assert.assertTrue(getBreadcrumbMessage()+" Child not found, "+list_workChild.get(wc).id+" child code "+list_workChild.get(wc).type.get("code"), childFound);
            }
        }
    }
      catch (NullPointerException e)
      {
          Assert.assertFalse(getBreadcrumbMessage()+e.toString(),true);
      }

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
    public HashMap<String, Object> getType() {return type;}
    public void setType(HashMap<String, Object> type) {this.type = type;}

    private String id;
    public String getId() {return id;}
    public void setId(String id) {this.id = id;}

    private WorkSummary workSummary;
    public WorkSummary getWorkSummary() {return workSummary;}
    public void setWorkSummary(WorkSummary workSummary) {this.workSummary = workSummary;}

    private String effectiveStartDate;
    public String getEffectiveStartDate() {return effectiveStartDate;}
    public void setEffectiveStartDate(String effectiveStartDate) {this.effectiveStartDate = effectiveStartDate;}

    private String effectiveEndDate;
    public String getEffectiveEndDate() {return effectiveEndDate;}
    public void setEffectiveEndDate(String effectiveEndDate) {this.effectiveEndDate = effectiveEndDate;}
  }

  public static class workChild {
    private HashMap<String, Object> type;
    public HashMap<String, Object> getType() {return type;}
    public void setType(HashMap<String, Object> type) {this.type = type;}

    private String id;
    public String getId() {return id;}
    public void setId(String id) {this.id = id;}

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

  private void printLog(String verified) {Log.info("verified..." + verified);}
}
