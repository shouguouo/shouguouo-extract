package com.shouguouo.extract.entity;

import java.util.HashMap;
import java.util.Map;

/**
 * @author shouguouo~
 * @date 2020/9/8 - 15:21
 */
public class Configuration {
    // 日期增量字段
    private String dateColumn;

    // 日期增量区间
    private int startDate;

    private int endDate;

    // 抽取的数据是否需要trim
    private boolean needTrim = false;

    // 入库数据库时间字段 "yyyyMMdd.HHmmss"格式
    private String timestamp;

    private Map<String, Object> additionalColumn = new HashMap<>();

    public String getDateColumn() {
        return dateColumn;
    }

    public void setDateColumn(String dateColumn) {
        this.dateColumn = dateColumn;
    }

    public int getStartDate() {
        return startDate;
    }

    public void setStartDate(int startDate) {
        this.startDate = startDate;
    }

    public int getEndDate() {
        return endDate;
    }

    public void setEndDate(int endDate) {
        this.endDate = endDate;
    }

    public boolean isNeedTrim() {
        return needTrim;
    }

    public void setNeedTrim(boolean needTrim) {
        this.needTrim = needTrim;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, Object> getAdditionalColumn() {
        return additionalColumn;
    }

    public void setAdditionalColumn(Map<String, Object> additionalColumn) {
        this.additionalColumn = additionalColumn;
    }

}
