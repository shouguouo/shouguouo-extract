package com.shouguouo.extract.enums;

import com.shouguouo.extract.util.ExtractException;

/**
 * @author shouguouo~
 * @date 2020/8/21 - 15:58
 */
public enum DatabaseType {
    Common(0, null, "pool.properties"),
    Oracle(1, "oracle.jdbc.OracleDriver", "oracle.properties"),
    Mysql(3, "com.mysql.jdbc.Driver", "mysql.properties");

    private int id;

    private String driverClassName;

    private String propertiesFileName;

    DatabaseType(int id, String driveClassName, String propertiesFileName) {
        this.id = id;
        this.driverClassName = driveClassName;
        this.propertiesFileName = propertiesFileName;
    }

    public int getId() {
        return id;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public String getPropertiesFileName() {
        return propertiesFileName;
    }

    public static DatabaseType toEnum(int id) {
        for (DatabaseType databaseType : DatabaseType.values()) {
            if (databaseType.id == id) {
                return databaseType;
            }
        }

        throw new ExtractException("UnSupport DatabaseType");
    }
}
