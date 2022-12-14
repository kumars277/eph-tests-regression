package com.eph.automation.testing.models.api;

public class WorkPackages {

    private IsInWorkPackages[] isInWorkPackages;
    public IsInWorkPackages[] getIsInWorkPackages() {return isInWorkPackages;}
    public void setIsInWorkPackages(IsInWorkPackages[] isInWorkPackages) {this.isInWorkPackages = isInWorkPackages;}

    private HasWorkComponents[] hasWorkComponents;
    public HasWorkComponents[] getHasWorkComponents() {return hasWorkComponents;}
    public void setHasWorkComponents(HasWorkComponents[] hasWorkComponents) {this.hasWorkComponents = hasWorkComponents;}

    public static class IsInWorkPackages
    {
        private String id;
        public String getId() {return id;}
        public void setId(String id) {this.id = id;}

        private String allocationPercentage;
        public String getAllocationPercentage() {return allocationPercentage;}
        public void setAllocationPercentage(String allocationPercentage) {this.allocationPercentage = allocationPercentage;}

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

    public static class HasWorkComponents
    {
        private String id;
        public String getId() {return id;}
        public void setId(String id) {this.id = id;}

        private String allocationPercentage;
        public String getAllocationPercentage() {return allocationPercentage;}
        public void setAllocationPercentage(String allocationPercentage) {this.allocationPercentage = allocationPercentage;}

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

}
