package com.shouguouo.extract.entity;

/**
 * @author shouguouo~
 * @date 2020/8/26 - 18:11
 */
public class TerminatedRecord extends Record {
    private static final TerminatedRecord SINGLE = new TerminatedRecord();
    private TerminatedRecord() {}

    public static TerminatedRecord get() {
        return SINGLE;
    }
}
