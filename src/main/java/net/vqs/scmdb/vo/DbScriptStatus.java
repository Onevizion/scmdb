package net.vqs.scmdb.vo;

public enum DbScriptStatus {
    EXECUTED(0), NOT_EXECUTED(1);

    int statusId;

    DbScriptStatus(int statusId) {
        this.statusId = statusId;
    }

    public int getStatusId() {
        return statusId;
    }

    public static DbScriptStatus getForInt(int statusId) {
        switch (statusId) {
            case 0:
                return EXECUTED;
            case 1:
                return NOT_EXECUTED;
            default:
                throw new IllegalArgumentException("Not supported db script statusId: [" + statusId + "]");
        }
    }
}
