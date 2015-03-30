package com.baina.game.analysis;

/**
 * Created by jjhu on 2015/3/16.
 */
public class RuleResultCell {
    private String date;
    private int banType;
    private String bannedKey;
    private String beforeTime;

    public String getBeforeTime() {
        return beforeTime;
    }

    public void setBeforeTime(String beforeTime) {
        this.beforeTime = beforeTime;
    }

    public String getBannedKey() {
        return bannedKey;
    }

    public void setBannedKey(String bannedKey) {
        this.bannedKey = bannedKey;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public int getBanType() {
        return banType;
    }

    public void setBanType(int banType) {
        this.banType = banType;
    }
}
