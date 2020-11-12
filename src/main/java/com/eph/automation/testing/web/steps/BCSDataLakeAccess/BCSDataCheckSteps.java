//created by Nishant @ 29 Sep 2020
package com.eph.automation.testing.web.steps.BCSDataLakeAccess;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.BCSDataQualityContext;
import com.eph.automation.testing.services.db.BCSDataLakeSQL.BCSDataLakeDataCheckSQL;
import com.eph.automation.testing.models.dao.BCSDataLake.*;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BCSDataCheckSteps {

    private static String sql;
    private static List<String> Ids;
    private static BCSDataQualityContext bcsDataQualityContext= new BCSDataQualityContext();

    @Given("^We get the (.*) random ids from initial ingest (.*)$")
    public void getRandomIdsFromInitialIngest(String countOfRandomIds, String targetTable){
        Log.info("getting random reference ids from initial ingest...");
    switch(targetTable)
    {
        case "stg_current_classification"       :
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_classification, countOfRandomIds);break;
        case "stg_current_content"              :
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_content, countOfRandomIds);break;
        case "stg_current_extobject"            :
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_extobject, countOfRandomIds);break;
        case "stg_current_fullversionfamily"    :
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_fullversionfamily, countOfRandomIds);break;
        case "stg_current_originatoraddress"    :
        sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_originatoraddress, countOfRandomIds);break;
        case "stg_current_originators"          :
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_originators, countOfRandomIds);break;
        case "stg_current_pricing"              :
            sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTableFor_stg_current_pricing, countOfRandomIds);break;
        case "stg_current_product"              :  break;
        case "stg_current_production"           :  break;
        case "stg_current_relations"            :  break;
        case "stg_current_responsibilities"     :  break;
        case "stg_current_sublocation"          :  break;
        case "stg_current_text"                 :  break;
        case "stg_current_versionfamily"        :  break;
    }

        List<Map<?, ?>> randomEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
    if(targetTable.equalsIgnoreCase("stg_current_originatoraddress"))
        Ids = randomEPRIds.stream().map(m -> (Integer) m.get("businesspartnerid")).map(String::valueOf).collect(Collectors.toList());
    else
    Ids = randomEPRIds.stream().map(m -> (String) m.get("sourceref")).collect(Collectors.toList());
        Log.info("Randomly picked ids..."+ Ids);
       //added by Nishant to debug failures
      //  Ids.clear();Ids.add("904");
    }

    @When("Get the data records from initial ingest for (.*)")
    public void getInitialIngestData(String targetTable){
        Log.info("We get initial ingest records...");

        switch(targetTable)
        {
            case "stg_current_classification"       :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_classification,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject .class, Constants.AWS_URL);
                bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getSourceref));break;

            case "stg_current_content"              :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_content,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject .class, Constants.AWS_URL);
                bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getSourceref));break;

            case "stg_current_extobject"            :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_extobject,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject .class, Constants.AWS_URL);
                bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getComments));break;

            case "stg_current_fullversionfamily"    :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_fullversionfamily,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject .class, Constants.AWS_URL);
                bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getProjectno));break;

            case "stg_current_originatoraddress"    :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_originatoraddress,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject .class, Constants.AWS_URL);
                bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getMetamodifiedon));break;

            case "stg_current_originators"          :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_originators,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject .class, Constants.AWS_URL);
             //   bcsDataQualityContext.bcsInitialIngestDataObjectList.sort(Comparator.comparing(BCSInitialIngestDataObject::getMetamodifiedon));break;


            case "stg_current_pricing"              :
                sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestDataFor_stg_current_pricing,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject .class, Constants.AWS_URL);

            case "stg_current_product"              :  break;
            case "stg_current_production"           :  break;
            case "stg_current_relations"            :  break;
            case "stg_current_responsibilities"     :  break;
            case "stg_current_sublocation"          :  break;
            case "stg_current_text"                 :  break;
            case "stg_current_versionfamily"        :  break;

        }

   }

    @Then ("^Get the records from current tables (.*)$")
    public void getCurrentTableData(String targetTable)    {//created by Nishant @ 21 Oct 2020
        Log.info("We get current table records...");
        switch(targetTable)
        {
            case "stg_current_classification"       :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_classification,
                Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList=DBManager.getDBResultAsBeanList(sql,BCSCurrentTableDataObject.class,Constants.AWS_URL);
                bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSourceref));break;

            case "stg_current_content"              :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_content,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList=DBManager.getDBResultAsBeanList(sql,BCSCurrentTableDataObject.class,Constants.AWS_URL);
                bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getSourceref));break;

            case "stg_current_extobject"            :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_extobject,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList=DBManager.getDBResultAsBeanList(sql,BCSCurrentTableDataObject.class,Constants.AWS_URL);
                bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getComments));break;

            case "stg_current_fullversionfamily"    :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_fullversionfamily,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList=DBManager.getDBResultAsBeanList(sql,BCSCurrentTableDataObject.class,Constants.AWS_URL);
                bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getProjectno));break;

            case "stg_current_originatoraddress"    :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_originatoraddress,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList=DBManager.getDBResultAsBeanList(sql,BCSCurrentTableDataObject.class,Constants.AWS_URL);
                bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getMetamodifiedon));break;

            case "stg_current_originators"          :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_originators,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList=DBManager.getDBResultAsBeanList(sql,BCSCurrentTableDataObject.class,Constants.AWS_URL);
             //   bcsDataQualityContext.bcsCurrentTableDataObjectList.sort(Comparator.comparing(BCSCurrentTableDataObject::getMetamodifiedon));break;

            case "stg_current_pricing"              :
                sql=String.format(BCSDataLakeDataCheckSQL.getCurrentTableDataFor_stg_current_pricing,
                        Joiner.on("','").join(Ids));
                bcsDataQualityContext.bcsCurrentTableDataObjectList=DBManager.getDBResultAsBeanList(sql,BCSCurrentTableDataObject.class,Constants.AWS_URL);

            case "stg_current_product"              :  break;
            case "stg_current_production"           :  break;
            case "stg_current_relations"            :  break;
            case "stg_current_responsibilities"     :  break;
            case "stg_current_sublocation"          :  break;
            case "stg_current_text"                 :  break;
            case "stg_current_versionfamily"        :  break;
        }
    }

    @And("Compare the records of initial ingest and current table (.*)")
    public void compareInitialIngestWithCurrentTable(String targetTable) {//created by Nishant @ 21 Oct 2020
        if (!bcsDataQualityContext.bcsInitialIngestDataObjectList.isEmpty() |
                !bcsDataQualityContext.bcsCurrentTableDataObjectList.isEmpty()) {

            Assert.assertEquals("initial ingest data count match with " + targetTable + "\n"
                            + "Initial ingest count = " + bcsDataQualityContext.bcsInitialIngestDataObjectList.size()
                            + "current table count= " + bcsDataQualityContext.bcsCurrentTableDataObjectList.size()
                    , bcsDataQualityContext.bcsInitialIngestDataObjectList.size(),
                    bcsDataQualityContext.bcsCurrentTableDataObjectList.size());

            switch(targetTable)
            {
                case "stg_current_classification"       :  compareIngestVsCurrentClassification();break;
                case "stg_current_content"              :  compareIngestVsCurrentContent();break;
                case "stg_current_extobject"            :  compareIngestVsCurrentExtobject();break;
                case "stg_current_fullversionfamily"    :  compareIngestVsCurrentFullVersionFamily();break;
                case "stg_current_originatoraddress"    :  compareIngestVsCurrentOriginatoraddress();break;
                case "stg_current_originators"          :  compareIngestVsCurrentOriginators();break;
                case "stg_current_pricing"              :  compareIngestVsCurrentPricing();break;
                case "stg_current_product"              :  break;
                case "stg_current_production"           :  break;
                case "stg_current_relations"            :  break;
                case "stg_current_responsibilities"     :  break;
                case "stg_current_sublocation"          :  break;
                case "stg_current_text"                 :  break;
                case "stg_current_versionfamily"        :  break;
            }
        }
    }

    public void compareIngestVsCurrentClassification(){//created by Nishant @ 21 Oct 2020
            for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++){
                boolean classificationcodeFound=false;
                for(int c=0;c<bcsDataQualityContext.bcsCurrentTableDataObjectList.size();c++) {
                    if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassificationcode()
                            .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getClassificationcode()))
                    {
                        classificationcodeFound=true;

                        Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                                (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetadeleted()));

                        Assert.assertEquals("metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                                (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetamodifiedon()));

                        Assert.assertEquals("sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                                (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSourceref()));

                        Assert.assertEquals("value mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getValue(),
                                (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getValue()));

                        Assert.assertEquals("classificationtype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassificationtype(),
                                (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getClassificationtype()));

                        Assert.assertEquals("priority mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPriority(),
                                (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getPriority()));

                        Assert.assertEquals("businessunit mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinessunit(),
                                (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getBusinessunit()));

                        Log.info("verified data at position " +i);
                        break;
                    }
                }
                   Assert.assertTrue("classificationCode value missing in current table"
                        +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassificationcode(),classificationcodeFound);
            }
        }

    public void compareIngestVsCurrentContent(){//created by Nishant @ 22 Oct 2020
        Log.info("verifying initial ingest Vs Current content...\n");
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++)
        {
            Log.info((i+1)+". verification for sourceref "+bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref());
            Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetadeleted()));
            printLog("Metadeleted");

            Assert.assertEquals("metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getMetamodifiedon()));
            printLog("metamodifiedon");

            Assert.assertEquals("sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSourceref()));
            printLog("sourceref");

            Assert.assertEquals("approvedondate mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getApprovedondate(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getApprovedondate()));
            printLog("approvedondate");


            Assert.assertEquals("companygroup mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCompanygroup(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCompanygroup()));
            printLog("companygroup");


            Assert.assertEquals("copyrightyear mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCopyrightyear(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getCopyrightyear()));
            printLog("copyrightyear");


            Assert.assertEquals("division mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getDivision(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getDivision()));
            printLog("division");

            Assert.assertEquals("doi mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getDoi(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getDoi()));
            printLog("doi");

            Assert.assertEquals("doistatus mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getDoistatus(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getDoistatus()));
            printLog("doistatus");


            Assert.assertEquals("editionid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getEditionid(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getEditionid()));
            printLog("editionid");


            Assert.assertEquals("editionno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getEditionno(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getEditionno()));
            printLog("editionno");

            Assert.assertEquals("firstapproval mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFirstapproval(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getFirstapproval()));
            printLog("firstapproval");

            Assert.assertEquals("impressionid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getImpressionid(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getImpressionid()));
            printLog("impressionid");

            Assert.assertEquals("imprint mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getImprint(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getImprint()));
            printLog("imprint");

            Assert.assertEquals("isset mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsset(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getIsset()));
            printLog("isset");

            Assert.assertEquals("language mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getLanguage(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLanguage()));
            printLog("language");

            Assert.assertEquals("language2 mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getLanguage2(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLanguage2()));
            printLog("language2");

            Assert.assertEquals("language3 mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getLanguage3(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getLanguage3()));
            printLog("language3");

            Assert.assertEquals("objecttype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getObjecttype(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getObjecttype()));
            printLog("objecttype");

            Assert.assertEquals("originimpid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getOriginimpid(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getOriginimpid()));
            printLog("originimpid");

            Assert.assertEquals("ownership mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getOwnership(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getOwnership()));
            printLog("ownership");

            Assert.assertEquals("piidack mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPiidack(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPiidack()));
            printLog("piidack");

            Assert.assertEquals("publisher mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPublisher(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getPublisher()));
            printLog("publisher");

            Assert.assertEquals("regstatus mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getRegstatus(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getRegstatus()));
            printLog("regstatus");

            Assert.assertEquals("series mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSeries(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSeries()));
            printLog("series");

            Assert.assertEquals("seriescode mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSeriescode(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSeriescode()));
            printLog("seriescode");

            Assert.assertEquals("seriesid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSeriesid(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSeriesid()));
            printLog("seriesid");

            Assert.assertEquals("seriesissn mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSeriesissn(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSeriesissn()));
            printLog("seriesissn");

            Assert.assertEquals("shorttitle mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getShorttitle(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getShorttitle()));
            printLog("shorttitle");

            Assert.assertEquals("subgroup mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSubgroup(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSubgroup()));
            printLog("subgroup");

            Assert.assertEquals("subtitle mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSubtitle(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSubtitle()));
            printLog("subtitle");

            Assert.assertEquals("synctemplate mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSynctemplate(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getSynctemplate()));
            printLog("synctemplate");

            Assert.assertEquals("title mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTitle(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTitle()));
            printLog("title");

            Assert.assertEquals("titleid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTitleid(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getTitleid()));
            printLog("titleid");

            Assert.assertEquals("volumename mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getVolumename(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getVolumename()));
            printLog("volumename");

            Assert.assertEquals("volumeno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getVolumeno(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getVolumeno()));
            printLog("volumeno");

            Assert.assertEquals("work_master_flag mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWork_master_flag(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWork_master_flag()));
            printLog("work_master_flag");

            Assert.assertEquals("worktitle mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWorktitle(),
                    (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(i).getWorktitle()));
            printLog("worktitle");
            Log.info("------------------------------------------");
        }
    }

    public void compareIngestVsCurrentExtobject(){//created by Nishant @ 22 Oct 2020
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++){
            boolean Found=false;
            for(int c=0;c<bcsDataQualityContext.bcsCurrentTableDataObjectList.size();c++) {
                if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSource()
                        .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSource()))
                {
                    Log.info("verification for - "+bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getComments());
                    Found=true;

                    Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetadeleted()));
                    printLog("Metadeleted");

                    Assert.assertEquals("metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetamodifiedon()));
                    printLog("metamodifiedon");

                    Assert.assertEquals("sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSourceref()));
                    printLog("sourceref");

                    Assert.assertEquals("object mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getObject(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getObject()));
                    printLog("object");

                    Assert.assertEquals("type mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getType(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getType()));
                    printLog("type");

                    Assert.assertEquals("name mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getName(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getName()));
                    printLog("name");

                    Assert.assertEquals("comments mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getComments(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getComments()));
                    printLog("comments");

                    Assert.assertEquals("source mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSource(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSource()));
                    printLog("source");

                    Log.info("------------------------------------------");
                    break;
                }

            }
            Assert.assertTrue("source missing in current table"
                    +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassificationcode(),Found);
        }

    }

    public void compareIngestVsCurrentFullVersionFamily(){//created by Nishant @ 05 Nov 2020
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++){
            boolean Found=false;
            for(int c=0;c<bcsDataQualityContext.bcsCurrentTableDataObjectList.size();c++) {
                if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getProjectno()
                        .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getProjectno()))
                {
                    Log.info("verification for projectno - "+bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getProjectno());
                    Found=true;

                    Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetadeleted()));
                    printLog("Metadeleted");

                    Assert.assertEquals("metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetamodifiedon()));
                    printLog("metamodifiedon");

                    Assert.assertEquals("sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSourceref()));
                    printLog("sourceref");

                    Assert.assertEquals("versiontype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getVersiontype(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getVersiontype()));
                    printLog("versiontype");

                    Assert.assertEquals("editionno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getEditionno(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getEditionno()));
                    printLog("editionno");

                    Assert.assertEquals("isbn mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsbn(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getIsbn()));
                    printLog("isbn");

                    Assert.assertEquals("projectno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getProjectno(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getProjectno()));
                    printLog("projectno");

                    Assert.assertEquals("workmaster mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getWorkmaster(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getWorkmaster()));
                    printLog("workmaster");

                    Log.info("------------------------------------------");
                    break;
                }
            }
            Assert.assertTrue("projectno missing in current table"
                    +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getProjectno(),Found);
        }
        Log.info("total "+bcsDataQualityContext.bcsInitialIngestDataObjectList.size()+ "projectno(s) varified for sourceref"
                +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }

    public void compareIngestVsCurrentOriginatoraddress(){//created by Nishant @ 6 Nov 2020
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++){
            boolean Found=false;
            for(int c=0;c<bcsDataQualityContext.bcsCurrentTableDataObjectList.size();c++) {
                if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon()
                        .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetamodifiedon()))
                {
                    Log.info("verification for metamodifiedon - "+bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon());
                    Found=true;

                    Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetadeleted()));
                    printLog("Metadeleted");

                    Assert.assertEquals("additionaladdress mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getAdditionaladdress(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getAdditionaladdress()));
                    printLog("additionaladdress");

                    Assert.assertEquals("addressid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getAddressid(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getAddressid()));
                    printLog("addressid");

                    Assert.assertEquals("addressline1 mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getAddressline1(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getAddressline1()));
                    printLog("addressline1");

                    Assert.assertEquals("addressline2 mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getAddressline2(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getAddressline2()));
                    printLog("addressline2");

                    Assert.assertEquals("addressline3 mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getAddressline3(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getAddressline3()));
                    printLog("addressline3");

                    Assert.assertEquals("businesspartnerid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getBusinesspartnerid()));
                    printLog("businesspartnerid");

                    Assert.assertEquals("city mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCity(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getCity()));
                    printLog("city");

                    Assert.assertEquals("country mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCountry(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getCountry()));
                    printLog("country");

                    Assert.assertEquals("district mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getDistrict(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getDistrict()));
                    printLog("district");

                    Assert.assertEquals("email mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getEmail(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getEmail()));
                    printLog("email");

                    Assert.assertEquals("fax mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFax(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getFax()));
                    printLog("fax");

                    Assert.assertEquals("houseno mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getHouseno(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getHouseno()));
                    printLog("houseno");

                    Assert.assertEquals("internet mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getInternet(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getInternet()));
                    printLog("internet");

                    Assert.assertEquals("ismainaddress mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsmainaddress(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getIsmainaddress()));
                    printLog("ismainaddress");

                    Assert.assertEquals("mobile mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMobile(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMobile()));
                    printLog("mobile");

                    Assert.assertEquals("postalcode mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPostalcode(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getPostalcode()));
                    printLog("postalcode");

                    Assert.assertEquals("street mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getStreet(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getStreet()));
                    printLog("street");

                    Assert.assertEquals("telephonemain mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTelephonemain(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getTelephonemain()));
                    printLog("telephonemain");

                    Assert.assertEquals("telephoneother mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getTelephoneother(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getTelephoneother()));
                    printLog("telephoneother");

                    Log.info("------------------------------------------");
                    break;
                }
            }
            Assert.assertTrue("modifiedon value missing in current table"
                    +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),Found);
        }
        Log.info("total "+bcsDataQualityContext.bcsInitialIngestDataObjectList.size()+ " modified entries varified for businesspartnerid "
                +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getBusinesspartnerid());

    }

    public void compareIngestVsCurrentOriginators(){//created by Nishant @ 11 Nov 2020
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++){
            boolean Found=false;
            for(int c=0;c<bcsDataQualityContext.bcsCurrentTableDataObjectList.size();c++) {
                if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid()
                        .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getBusinesspartnerid()))
                {
                    Log.info("verification for businessPartnerId - "+bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid());
                    Found=true;

                    Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetadeleted()));
                    printLog("Metadeleted");

                    Assert.assertEquals("prefix mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPrefix(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getPrefix()));
                    printLog("prefix");

                    Assert.assertEquals("sequence mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSequence(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSequence()));
                    printLog("sequence");

                    Assert.assertEquals("businesspartnerid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getBusinesspartnerid()));
                    printLog("businesspartnerid");

                    Assert.assertEquals("originatorid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getOriginatorid(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getOriginatorid()));
                    printLog("originatorid");

                    Assert.assertEquals("isperson mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getIsperson(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getIsperson()));
                    printLog("isperson");

                    Assert.assertEquals("locationid mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getLocationid(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getLocationid()));
                    printLog("locationid");

                    Assert.assertEquals("copyrightholdertype mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCopyrightholdertype(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getCopyrightholdertype()));
                    printLog("copyrightholdertype");

                    Assert.assertEquals("institution mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getInstitution(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getInstitution()));
                    printLog("institution");

                    Assert.assertEquals("firstname mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getFirstname(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getFirstname()));
                    printLog("firstname");

                    Assert.assertEquals("department mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getDepartment(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getDepartment()));
                    printLog("department");

                    Assert.assertEquals("lastname mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getLastname(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getLastname()));
                    printLog("lastname");

                    Assert.assertEquals("searchterm mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSearchterm(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSearchterm()));
                    printLog("searchterm");

                    Log.info("------------------------------------------");
                    break;
                }
            }
            Assert.assertTrue("businessparternedId missing in current table"
                    +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid(),Found);
        }
        Log.info("total "+bcsDataQualityContext.bcsInitialIngestDataObjectList.size()+ " modified entries varified for sourceref "
                +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }


    public void compareIngestVsCurrentPricing(){
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++){
            boolean Found=false;
            for(int c=0;c<bcsDataQualityContext.bcsCurrentTableDataObjectList.size();c++) {
                if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon()
                        .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetamodifiedon()))
                {
                    Log.info("verification for modifiendon - "+bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon());
                    Found=true;

                    Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetadeleted()));
                    printLog("Metadeleted");

                    Assert.assertEquals("metamodifiedon mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetamodifiedon()));
                    printLog("metamodifiedon");
                    Assert.assertEquals("sourceref mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getSourceref(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getSourceref()));
                    printLog("sourceref");

                    Assert.assertEquals("validfrom mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getValidfrom(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getValidfrom()));
                    printLog("validfrom");

                    Assert.assertEquals("type mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getType(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getType()));
                    printLog("type");

                    Assert.assertEquals("currency mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getCurrency(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getCurrency()));
                    printLog("currency");

                    Assert.assertEquals("priceapprox mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPriceapprox(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getPriceapprox()));
                    printLog("priceapprox");

                    Assert.assertEquals("price mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getPrice(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getPrice()));
                    printLog("price");

                    Assert.assertEquals("validto mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getValidto(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getValidto()));
                    printLog("validto");


                    Log.info("------------------------------------------");
                    break;
                }
            }
            Assert.assertTrue("modifiedon value missing in current table"
                    +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetamodifiedon(),Found);
        }
        Log.info("total "+bcsDataQualityContext.bcsInitialIngestDataObjectList.size()+ " modified entries varified for sourceref "
                +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }


    public void compareIngestVsCurrentTableName(){
        for(int i= 0; i<bcsDataQualityContext.bcsInitialIngestDataObjectList.size();i++){
            boolean Found=false;
            for(int c=0;c<bcsDataQualityContext.bcsCurrentTableDataObjectList.size();c++) {
                if (bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassificationcode()
                        .equalsIgnoreCase(bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getClassificationcode()))
                {
                    Log.info("verification for businessPartnerId - "+bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getBusinesspartnerid());
                    Found=true;

                    Assert.assertEquals("Metadeleted mismatch ", bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getMetadeleted(),
                            (bcsDataQualityContext.bcsCurrentTableDataObjectList.get(c).getMetadeleted()));
                    printLog("Metadeleted");
                    Log.info("------------------------------------------");
                    break;
                }
            }
            Assert.assertTrue("classificationCode value missing in current table"
                    +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(i).getClassificationcode(),Found);
        }
        Log.info("total "+bcsDataQualityContext.bcsInitialIngestDataObjectList.size()+ " modified entries varified for sourceref "
                +bcsDataQualityContext.bcsInitialIngestDataObjectList.get(0).getSourceref());
    }


    private void printLog(String verified){Log.info("verified..."+verified);}
}
