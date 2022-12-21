package com.eph.automation.testing.models.dao.StitchingExtended;

public class WorkExtPersonRoleJson {

    private String sequenceNumber;
    private String coreWorkPersonRoleId;
    public String getSequenceNumber() {
        return sequenceNumber;}
    public void setSequenceNumber(String sequenceNumber) {this.sequenceNumber = sequenceNumber;}

    public String getCoreWorkPersonRoleId() {
        return coreWorkPersonRoleId;}
    public void setCoreWorkPersonRoleId(String coreWorkPersonRoleId) {this.coreWorkPersonRoleId = coreWorkPersonRoleId;}

    private ExtendedRole extendedRole;
    public ExtendedRole getExtendedRole() {return extendedRole;}
    public void setExtendedRole(ExtendedRole extendedRole) {this.extendedRole = extendedRole;}

    private ExtendedPerson extendedPerson;
    public ExtendedPerson getExtendedPerson() {return extendedPerson;}
    public void setExtendedPerson(ExtendedPerson extendedPerson) {this.extendedPerson = extendedPerson;}


    public static class ExtendedRole{
        /*private HashMap<String ,Object> type;
        public HashMap<String, Object> getType() {return type;}
        public void setType(HashMap<String, Object> type) {this.type = type;}
*/
        private String code;
        private String name;



        public String getCode() {
            return code;}
        public void setCode(String code) {this.code = code;}

        public String getName() {
            return name;}
        public void setName(String name) {this.name = name;}


    }

    public static class ExtendedPerson{
            private String firstName;
            private String lastName;
            private String honours;
            private String affiliation;
            private String imageUrl;
            private String email;

        public String getFirstName() {
            return firstName;}
        public void setFirstName(String firstName) {this.firstName = firstName;}

        public String getEmail() {
            return email;}
        public void setEmail(String email) {this.email = email;}

        public String getLastName() {
            return lastName;}
        public void setLastName(String lastName) {this.lastName = lastName;}

        public String getHonours() {
            return honours;}
        public void setHonours(String honours) {this.honours = honours;}

        public String getAffiliation() {
            return affiliation;}
        public void setAffiliation(String affiliation) {this.affiliation = affiliation;}

        public String getImageUrl() {
            return imageUrl;}
        public void setImageUrl(String imageUrl) {this.imageUrl = imageUrl;}

    }

}




