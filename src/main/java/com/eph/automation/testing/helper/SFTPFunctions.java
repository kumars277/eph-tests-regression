package com.eph.automation.testing.helper;

import features.zOnHold.talend.TalendServerVariables;
import com.jcraft.jsch.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

/**
 * Created by RAVIVARMANS on 29/11/2018.
 */
public class SFTPFunctions implements TalendServerVariables {

    public static void SFTPUpload(String filePath, String sftpDropFolder)
            throws JSchException, FileNotFoundException, SftpException {
        performSFTPUpload(filePath, sftpDropFolder,SFTPHOST);
    }

    public static void SFTPUpload(String filePath, String sftpDropFolder, String SFTPHost)
            throws JSchException, FileNotFoundException, SftpException {
        performSFTPUpload(filePath, sftpDropFolder,SFTPHost);
    }

    public static void performSFTPUpload(String filePath, String sftpDropFolder, String SFTPHost) {
        Session session = null;
        Channel channel = null;
        ChannelSftp channelSftp = null;
        System.out.println("preparing the host information for sftp.");
        try {
            JSch jsch = new JSch();
            try {
                jsch.addIdentity(privateKey);
            } catch(Exception e) {
                try {
                    jsch.addIdentity("~/.ssh/id_rsa");
                } catch (Exception ex) {
                    jsch.addIdentity(privateRootKey);
                }
            }
            session = jsch.getSession(SFTPUSER, SFTPHost, SFTPPORT);
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();
            System.out.println("Host connected.");
            channel = session.openChannel("sftp");
            channel.connect();
            System.out.println("sftp channel opened and connected.");
            channelSftp = (ChannelSftp) channel;
            System.out.println("sftp channel created");
            channelSftp.cd(sftpDropFolder);
            System.out.println(sftpDropFolder);
            File f = new File(filePath);
            channelSftp.put(new FileInputStream(f), f.getName());
            System.out.println("file uploaded");
        } catch (Exception exc) {
            System.out.println(exc.toString());
        } finally {
            channelSftp.exit();
            System.out.println("sftp Channel exited.");
            channel.disconnect();
            System.out.println("Channel disconnected.");
            session.disconnect();
            System.out.println("Host Session disconnected.");
        }
    }

}
