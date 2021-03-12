package com.eph.automation.testing.models.dao.StitchingExtended;
import java.util.HashMap;

public class Applications {

    private String applicationName;
    private String deltaAnswerCodeUK;
    private String deltaAnswerCodeUS;
    private String publicationStatusANZ;
    private String availabilityStartDate;
    private String availabilityStatus;
    private String availabilityFormat;

    public String getApplicationName() { return applicationName;  }
    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public String getAvailabilityFormat() { return availabilityFormat;  }
    public void setAvailabilityFormat(String availabilityFormat) {
        this.availabilityFormat = availabilityFormat;
    }

    public String getDeltaAnswerCodeUK() {return deltaAnswerCodeUK;}
    public void setDeltaAnswerCodeUK(String deltaAnswerCodeUK) {
        this.deltaAnswerCodeUK = deltaAnswerCodeUK;
    }

    public String getDeltaAnswerCodeUS() {return deltaAnswerCodeUS; }
    public void setDeltaAnswerCodeUS(String deltaAnswerCodeUS) {
        this.deltaAnswerCodeUS = deltaAnswerCodeUS;
    }

    public String getPublicationStatusANZ() {return publicationStatusANZ; }
    public void setPublicationStatusANZ(String publicationStatusANZ) {
        this.publicationStatusANZ = publicationStatusANZ;
    }

    public String getAvailabilityStartDate() {return availabilityStartDate; }
    public void setAvailabilityStartDate(String availabilityStartDate) {
        this.availabilityStartDate = availabilityStartDate;
    }

    public String getAvailabilityStatus() {return availabilityStatus; }
    public void setAailabilityStatus(String availabilityStatus) {
        this.availabilityStatus = availabilityStatus;
    }

    /*
    private String applicationName;
    public String getApplicationName() {return applicationName;}
    public void setApplicationName(String applicationName) {this.applicationName = applicationName;}
*/
   /* public static class ExtendedPageCount{
        private HashMap<String ,Object> type;
        public HashMap<String, Object> getType() {return type;}
        public void setType(HashMap<String, Object> type) {this.type = type;}


        private String count;
        public String getCount() {
            return count;}
        public void setCount(String count) {this.count = count;}
    }*/


}

