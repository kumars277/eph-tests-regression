package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 */
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkApiObject {
    private List<WorkDataObject> workDataObjectsFromEPHGD;

    public WorkApiObject() {
    }

    private String id;
    private String title;
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
    private AccountableProductApiObject accountableProduct;
    private FinancialAttributesApiObject[] financialAttributes;
    private WorkPersonsApiObject[] persons;
    private CopyrightOwnersApiObject[] copyrightOwner;
    private SubjectAreasApiObject[] subjectAreas;
    public WorkManifestationApiObject[] manifestations;

    public void compareWithDB(){
        getWorkDataFromEPHGD(this.id);
        Assert.assertEquals(this.title, this.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
        Assert.assertEquals(Integer.valueOf(this.volume), Integer.valueOf(this.workDataObjectsFromEPHGD.get(0).getVOLUME()));
        Assert.assertEquals(Integer.valueOf(this.editionNumber), Integer.valueOf(this.workDataObjectsFromEPHGD.get(0).getEDITION_NUMBER()));
        Assert.assertEquals(Integer.valueOf(this.copyrightYear), Integer.valueOf(this.workDataObjectsFromEPHGD.get(0).getCOPYRIGHT_YEAR()));
        Assert.assertEquals(Boolean.valueOf(this.electronicRightsInd), Boolean.valueOf(this.workDataObjectsFromEPHGD.get(0).getWORK_TITLE()));
//        for (WorkIdentifiersApiObject workId : this.identifiers){workId.compareWithDB();}
//        Assert.assertEquals(this.type.get("workTypeCode"), this.workDataObjectsFromEPHGD.get(0).getF_TYPE());
//        Assert.assertEquals(this.status.get("workStatusCode"), this.workDataObjectsFromEPHGD.get(0).getWORK_STATUS());
//        Assert.assertEquals(this.imprint.get("imprintCode"), this.workDataObjectsFromEPHGD.get(0).getIMPRINT());
//        this.pmc.compareWithDB();
//        for (WorkPersonsApiObject person : persons){person.compareWithDB();}
//        for (CopyrightOwnersApiObject cOwner : copyrightOwner){cOwner.compareWithDB();}
//        for (WorkManifestationApiObject manifestation : manifestations){manifestation.compareWithDB();}
    }


    public WorkPersonsApiObject[] getPersons() {
        return persons;
    }
    public void setPersons(WorkPersonsApiObject[] persons) {
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

    public String getVolume() {
        return volume;
    }

    public void setVolume(String volume) {
        this.volume = volume;
    }

    public String getCopyrightYear() {
        return copyrightYear;
    }

    public void setCopyrightYear(String copyrightYear) {
        this.copyrightYear = copyrightYear;
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

    public CopyrightOwnersApiObject[] getCopyrightOwner() {
        return copyrightOwner;
    }

    public void setCopyrightOwner(CopyrightOwnersApiObject[] copyrightOwner) {
        this.copyrightOwner = copyrightOwner;
    }


    public WorkManifestationApiObject[] getManifestations() {
        return manifestations;
    }

    public void setManifestations(WorkManifestationApiObject[] manifestations) {
        this.manifestations = manifestations;
    }

    private void getWorkDataFromEPHGD(String workID) {
        List<String> ids = new ArrayList<>();
        ids.add(workID);
        String sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_FOR_SEARCH, Joiner.on("','").join(ids));
        workDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    }

}