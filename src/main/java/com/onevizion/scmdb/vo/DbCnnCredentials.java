package com.onevizion.scmdb.vo;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DbCnnCredentials {
    private static final String JDBC_THIN_URL_PREFIX = "jdbc:oracle:thin:@";
    public static final String DB_CNN_STR_ERROR_MESSAGE = "You should specify db connection properties using following format:"
            + " <username>/<password>@<host>:<port>:<SID>";

    private String user;
    private String password;
    private String ownerCnnStr;
    private String userCnnStr;
    private String oracleUrl;

    private DbCnnCredentials() {}

    public static DbCnnCredentials create(String ownerCnnStr) {
        DbCnnCredentials cnnCredentials = new DbCnnCredentials();
        cnnCredentials.setOwnerCnnStr(ownerCnnStr);
        cnnCredentials.setUserCnnStr(genUserCnnStr(ownerCnnStr));

        Pattern p = Pattern.compile("(.+?)/(.+?)@(.+)");
        Matcher m = p.matcher(ownerCnnStr);
        if (m.matches() && m.groupCount() == 3) {
            cnnCredentials.setUser(m.group(1));
            cnnCredentials.setPassword(m.group(2));
            cnnCredentials.setOracleUrl(JDBC_THIN_URL_PREFIX + m.group(3));
        } else {
            throw new IllegalArgumentException(DB_CNN_STR_ERROR_MESSAGE);
        }
        return cnnCredentials;
    }

    private static String genUserCnnStr(String ownerCnnStr) {
        String owner = ownerCnnStr.substring(0, ownerCnnStr.indexOf("/"));
        String user = owner + "_user";
        return user + ownerCnnStr.substring(ownerCnnStr.indexOf("/"));
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getOwnerCnnStr() {
        return ownerCnnStr;
    }

    public String getUserCnnStr() {
        return userCnnStr;
    }

    public void setUserCnnStr(String userCnnStr) {
        this.userCnnStr = userCnnStr;
    }

    public void setOwnerCnnStr(String ownerCnnStr) {
        this.ownerCnnStr = ownerCnnStr;
    }

    public String getOracleUrl() {
        return oracleUrl;
    }

    public void setOracleUrl(String oracleUrl) {
        this.oracleUrl = oracleUrl;
    }
}
