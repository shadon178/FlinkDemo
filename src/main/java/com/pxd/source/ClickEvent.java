package com.pxd.source;

import java.sql.Timestamp;

public class ClickEvent {

    public String userName;

    public String url;

    public long timestamp;

    public ClickEvent() {
    }

    public ClickEvent(String userName, String url, long timestamp) {
        this.userName = userName;
        this.url = url;
        this.timestamp = timestamp;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "ClickEvent{" +
            "userName='" + userName + '\'' +
            ", url='" + url + '\'' +
            ", timestamp=" + new Timestamp(timestamp) +
            '}';
    }

}
