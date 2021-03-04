package com.eph.automation.testing.models.api;
//created by Nishant @ 29 Jan 2021,  EPHD-2747
public class AvailabilityExtendedAPIObj {

    private AvailabilityExtApplicationsAPIObj[] applications;
    public AvailabilityExtApplicationsAPIObj[] getApplications() {return applications;}
    public void setApplications(AvailabilityExtApplicationsAPIObj[] applications) {this.applications = applications;}

    public void compareApplications()
    {
        int i=0;
        for(AvailabilityExtApplicationsAPIObj app :applications)
        {
            app.compareWithJson(i);i++;
    }
    }

}
