package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 */
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkApiObject {
    private List<WorkDataObject> workDataObjectsFromEPHGD;

    public WorkApiObject() {
    }

    private String id;
    private String title;
    private String subTitle;
    private String electronicRightsInd;
    private HashMap<String,Object> language;
    private String editionNumber;
    private String volume;
    private String copyrightYear;
    private WorkIdentifiersApiObject[] identifiers;
    private HashMap<String,Object> type;
    private HashMap<String,Object> status;
    private HashMap<String,Object> imprint;
    private PMCApiObject pmc;
    private FinancialAttributesApiObject[] financialAttributes;
    private PersonsApiObject[] persons;
    private CopyrightOwnersApiObject[] copyrightOwner;
    private SubjectAreasApiObject[] subjectAreas;
    private WorkManifestationApiObject[] manifestations;



    public void compareWithDB(){
        getWorkDataFromEPHGD(this.id);
        Assert.assertEquals(this.subTitle, this.workDataObjectsFromEPHGD.get(0).getWORK_SUBTITLE());
        Assert.assertEquals(Boolean.valueOf(this.electronicRightsInd), Boolean.valueOf(this.workDataObjectsFromEPHGD.get(0).getELECTRONIC_RIGHTS_IND()));
        if(!(this.workDataObjectsFromEPHGD.get(0).getLANGUAGE_CODE()==null)) {
            Assert.assertEquals(this.language.get("code"), this.workDataObjectsFromEPHGD.get(0).getLANGUAGE_CODE());
        }
        if(!(this.editionNumber==null)){
            int apiEditionNumber = Integer.valueOf(this.editionNumber);
            Assert.assertEquals(apiEditionNumber, this.workDataObjectsFromEPHGD.get(0).getEDITION_NUMBER());
        }

        int apiVolume =Integer.valueOf(this.volume);
        Assert.assertEquals(apiVolume, this.workDataObjectsFromEPHGD.get(0).getVOLUME());

        if(this.workDataObjectsFromEPHGD.get(0).getCOPYRIGHT_YEAR()!=0) {
            int apiCopyrightYear = Integer.valueOf(this.copyrightYear);
            Assert.assertEquals(apiCopyrightYear, this.workDataObjectsFromEPHGD.get(0).getCOPYRIGHT_YEAR());
        }
//        if(this.identifiers!=null) {
//            for (WorkIdentifiersApiObject workIdentifier : this.identifiers) {
//                workIdentifier.compareWithDB();
//            }
//        }
        Assert.assertEquals(this.type.get("code"), this.workDataObjectsFromEPHGD.get(0).getWORK_TYPE());
        Assert.assertEquals(this.status.get("code"), this.workDataObjectsFromEPHGD.get(0).getWORK_STATUS());
        if(!(this.imprint.get("code")==null)||!((this.workDataObjectsFromEPHGD==null)||(this.workDataObjectsFromEPHGD.isEmpty()))) {
            Assert.assertEquals(this.imprint.get("code"), this.workDataObjectsFromEPHGD.get(0).getIMPRINT());
        }
        Assert.assertEquals(this.pmc.getCode(), this.workDataObjectsFromEPHGD.get(0).getPMC());
        Assert.assertEquals(this.pmc.getPmg().get("code"), getPMGcodeByPMC(this.workDataObjectsFromEPHGD.get(0).getPMC()));
        if(!(this.financialAttributes==null)&&!(this.financialAttributes.length==0)){
            for (FinancialAttributesApiObject attribute : this.financialAttributes) {
                attribute.compareWithDB(this.id);
            }
        }
        if(!(this.persons==null)&&!(this.persons.length==0)){
            for (PersonsApiObject person : persons) {
                person.compareWithDB_work();
            }
        }
        if(!(this.subjectAreas==null)&&!(this.subjectAreas.length==0)){
            for (SubjectAreasApiObject sa : this.subjectAreas) {
                sa.compareWithDB(this.id);
            }
        }
        for (WorkManifestationApiObject manifestation : manifestations){manifestation.compareWithDB();}
    }

    public List<WorkDataObject> getWorkDataObjectsFromEPHGD() {
        return workDataObjectsFromEPHGD;
    }

    public void setWorkDataObjectsFromEPHGD(List<WorkDataObject> workDataObjectsFromEPHGD) {
        this.workDataObjectsFromEPHGD = workDataObjectsFromEPHGD;
    }

    public String getSubTitle() {
        return subTitle;
    }

    public void setSubTitle(String subTitle) {
        this.subTitle = subTitle;
    }

    public HashMap<String, Object> getLanguage() {
        return language;
    }

    public void setLanguage(HashMap<String, Object> language) {
        this.language = language;
    }

    public FinancialAttributesApiObject[] getFinancialAttributes() {
        return financialAttributes;
    }

    public void setFinancialAttributes(FinancialAttributesApiObject[] financialAttributes) {
        this.financialAttributes = financialAttributes;
    }

    public SubjectAreasApiObject[] getSubjectAreas() {
        return subjectAreas;
    }

    public void setSubjectAreas(SubjectAreasApiObject[] subjectAreas) {
        this.subjectAreas = subjectAreas;
    }

    public PersonsApiObject[] getPersons() {
        return persons;
    }

    public void setPersons(PersonsApiObject[] persons) {
        this.persons = persons;
    }

    public PMCApiObject getPmc() {
        return pmc;
    }

    public void setPmc(PMCApiObject pmc) {
        this.pmc = pmc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getVolume() {
        return volume;
    }

    public void setVolume(String volume) {
        this.volume = volume;
    }

    public HashMap<String, Object> getType() {
        return type;
    }

    public void setType(HashMap<String, Object> type) {
        this.type = type;
    }

    public HashMap<String, Object> getStatus() {
        return status;
    }

    public void setStatus(HashMap<String, Object> status) {
        this.status = status;
    }

    public HashMap<String, Object> getImprint() {
        return imprint;
    }

    public void setImprint(HashMap<String, Object> imprint) {
        this.imprint = imprint;
    }

    public WorkIdentifiersApiObject[] getIdentifiers() {
        return identifiers;
    }

    public void setIdentifiers(WorkIdentifiersApiObject[] identifiers) {
        this.identifiers = identifiers;
    }

    public WorkManifestationApiObject[] getManifestations() {
        return manifestations;
    }

    public void setManifestations(WorkManifestationApiObject[] manifestations) {
        this.manifestations = manifestations;
    }


    public String getElectronicRightsInd() {
        return electronicRightsInd;
    }

    public void setElectronicRightsInd(String electronicRightsInd) {
        this.electronicRightsInd = electronicRightsInd;
    }

    public String getEditionNumber() {
        return editionNumber;
    }

    public void setEditionNumber(String editionNumber) {
        this.editionNumber = editionNumber;
    }

    public String getCopyrightYear() {
        return copyrightYear;
    }

    public void setCopyrightYear(String copyrightYear) {
        this.copyrightYear = copyrightYear;
    }

    public CopyrightOwnersApiObject[] getCopyrightOwner() {
        return copyrightOwner;
    }

    public void setCopyrightOwner(CopyrightOwnersApiObject[] copyrightOwner) {
        this.copyrightOwner = copyrightOwner;
    }


    private void getWorkDataFromEPHGD(String workID) {
        List<String> ids = new ArrayList<>();
        ids.add(workID);
        String sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_FOR_SEARCH, Joiner.on("','").join(ids));
        workDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    }

    private String getPMGcodeByPMC(String pmcCode) {
        String sql = String.format(APIDataSQL.EPH_GD_PMG_CODE_EXTRACT_BYPMC, pmcCode);
        //  Log.info(sql);
        List<Map<String, Object>> getPMG = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        String pmgCode = ((String) getPMG.get(0).get("f_pmg"));
        return pmgCode;
    }
}