package com.shouguouo.extract.enums;

public enum ExtractType {
    ALL(1),
    APPEND(2);

    private int type;

    ExtractType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public static ExtractType toEnum(int type) {
        for (ExtractType extractType : ExtractType.values()) {
            if (extractType.getType() == type) {
                return extractType;
            }
        }
        throw new RuntimeException("InValid Extract Type : valid type should be 1(all) or 2(append)");
    }
}
