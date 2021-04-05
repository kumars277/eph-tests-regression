package com.eph.automation.testing.models.api;

import com.eph.automation.testing.helper.Log;
import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

import static com.eph.automation.testing.models.contexts.DataQualityContext.randomIdsData;

//created by Nishant @ 29 Jan 2021, EPHD-2747
public class AvailabilityExtApplicationsAPIObj {
    private String applicationName;
    public String getApplicationName() {return applicationName;}
    public void setApplicationName(String applicationName) {this.applicationName = applicationName;}

    private String availabilityFormat;
    public String getAvailabilityFormat() {return availabilityFormat;}
    public void setAvailabilityFormat(String availabilityFormat) {this.availabilityFormat = availabilityFormat;}

    private String availabilityStartDate;
    public String getAvailabilityStartDate() {return availabilityStartDate;}
    public void setAvailabilityStartDate(String availabilityStartDate) {this.availabilityStartDate = availabilityStartDate;}

    private String availabilityStatus;
    public String getAvailabilityStatus() {return availabilityStatus;}
    public void setAvailabilityStatus(String availabilityStatus) {this.availabilityStatus = availabilityStatus;}

    private String deltaAnswerCodeUK;
    public String getDeltaAnswerCodeUK() {return deltaAnswerCodeUK;}
    public void setDeltaAnswerCodeUK(String deltaAnswerCodeUK) {this.deltaAnswerCodeUK = deltaAnswerCodeUK;}

    private String deltaAnswerCodeUS;
    public String getDeltaAnswerCodeUS() {return deltaAnswerCodeUS;}
    public void setDeltaAnswerCodeUS(String deltaAnswerCodeUS) {this.deltaAnswerCodeUS = deltaAnswerCodeUS;}

    private String publicationStatusANZ;
    public String getPublicationStatusANZ() {return publicationStatusANZ;}
    public void setPublicationStatusANZ(String publicationStatusANZ) {this.publicationStatusANZ = publicationStatusANZ;}




    @Override
    public int hashCode() {
        return Objects.hash(applicationName,availabilityFormat,availabilityStartDate, availabilityStatus, deltaAnswerCodeUK, deltaAnswerCodeUS, publicationStatusANZ);
    }
public void compareWithJson(int cnt)
{

    Log.info(this.applicationName);
    AvailabilityExtendedTestClass jsonValue = new Gson().fromJson(randomIdsData.get(cnt).get("json").toString(),AvailabilityExtendedTestClass.class);
    ArrayList<AvailabilityExtApplicationsAPIObj> user_array= new ArrayList<>(Arrays.asList((jsonValue.getAvailabilityExtended().getApplications())));

    Log.info("");

}

}
