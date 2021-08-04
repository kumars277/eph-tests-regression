package features.zOnHold.talend;

/**
 * Created by RAVIVARMANS on 11/29/2018.
 */
public interface TalendServerVariables {

    String inputDirPrj008Job200 = "/apps/talend/projects/PRJ_008_CUSTOMERINSIGHT_ETL/data/incoming";
    String inputDirPrj116JobARGIHS = "/apps/talend/projects/TDMP_116_ARGI_TO_CUSTOMER_HUB/data/incoming";
    String inputDirPrj117JobDELTROW = "/apps/talend/projects/TDMP_117_DELTA_TO_CUSTOMER_HUB/data/incoming";
    String inputDirPrj114Job200 = "/apps/talend/projects/PRJ_114_RG_TO_CUSTOMER_HUB/data/incoming";
    String inputDirPrj113AmpsJob010 = "/apps/talend/projects/PRJ_113_AMPS_TO_CUSTOMER_HUB/data/incoming";
    String privateKey = "src/main/resources/ppk/id_rsa.ppk";
    String privateRootKey = "src/main/resources/ppk/root/id_rsa.ppk";
    String SFTPHOST = "jobserver.sit.ifp.elsevier.com";
    String SFTPUATHOST = "jobserver.uat.ifp.elsevier.com";
    String OLD_SFTPHOST = "tdmp.sit.elsevier.com";
    int SFTPPORT = 22;
    String SFTPUSER = "talenduser";
    String OLD_SFTPUSER = "talend";

}
