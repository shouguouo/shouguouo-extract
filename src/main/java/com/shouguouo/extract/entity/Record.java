package com.shouguouo.extract.entity;

import java.util.Map;

/**
 * @author shouguouo~
 * @date 2020/8/26 - 18:10
 */
public class Record {
    private Map<String, Object> record;

    public Record() {
    }

    public Record(Map<String, Object> record) {
        this.record = record;
    }

    public Map<String, Object> getRecord() {
        return record;
    }

    public void setRecord(Map<String, Object> record) {
        this.record = record;
    }
}
