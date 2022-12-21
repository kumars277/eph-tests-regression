package com.eph.automation.testing.models.dao.StitchingExtended;

public class AvailabilityJson {

    private Applications applications;
    public Applications getApplications() {return applications;}
    public void setApplications(Applications applications) {this.applications = applications;}

    public static class Applications{
        private String applicationName;
        public String getapplicationName() {
            return applicationName;}
        public void setapplicationName(String applicationName) {this.applicationName = applicationName;}
    }


}

