package com.eph.automation.testing.models.dao.StitchingExtended;

public class WorkExtEditorialBoardJson {

    private String groupNumber;
    private String sequenceNumber;


    public String getGroupNumber() { return groupNumber; }
    public void setGroupNumber(String groupNumber) { this.groupNumber = groupNumber; }

    public String getSequenceNumber() { return sequenceNumber; }
    public void setSequenceNumber(String sequenceNumber) { this.sequenceNumber = sequenceNumber; }

    private ExtendedBoardRole extendedBoardRole;
    public ExtendedBoardRole getExtendedBoardRole() {return extendedBoardRole;}
    public void setExtendedBoardRole(ExtendedBoardRole extendedBoardRole) {this.extendedBoardRole = extendedBoardRole;}

    private ExtendedBoardMember extendedBoardMember;
    public ExtendedBoardMember getExtendedBoardMember() {return extendedBoardMember;}
    public void setExtendedBoardMember(ExtendedBoardMember extendedBoardMember) {this.extendedBoardMember = extendedBoardMember;}


    public static class ExtendedBoardRole {
//        private HashMap<String ,Object> type;
//        public HashMap<String, Object> getType() {return type;}
//        public void setType(HashMap<String, Object> type) {this.type = type;}

        private String code;
        public String getCode() { return code; }
        public void setCode(String code) { this.code = code; }
    }

        public static class ExtendedBoardMember{
            private String firstName;
            private String lastName;
            private String title;
            private String affiliation;
            private String imageUrl;
            private String notes;

            public String getFirstName() {
                return firstName;}
            public void setFirstName(String firstName) {this.firstName = firstName;}

            public String getNotes() {
                return notes;}
            public void setNotes(String notes) {this.notes = notes;}

            public String getLastName() {
                return lastName;}
            public void setLastName(String lastName) {this.lastName = lastName;}

            public String getTitle() {
                return title;}
            public void setTitle(String title) {this.title = title;}

            public String getAffiliation() {
                return affiliation;}
            public void setAffiliation(String affiliation) {this.affiliation = affiliation;}

            public String getImageUrl() {
                return imageUrl;}
            public void setImageUrl(String imageUrl) {this.imageUrl = imageUrl;}

        }

    }





