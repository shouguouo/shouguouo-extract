package com.shouguouo.extract.database;


import com.shouguouo.extract.database.dialect.Dialect;
import com.shouguouo.extract.database.dialect.MysqlDialect;
import com.shouguouo.extract.database.dialect.OracleDialect;
import com.shouguouo.extract.enums.DatabaseType;
import com.shouguouo.extract.util.ExtractException;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * @author shouguouo~
 * @date 2020/8/21 - 16:05
 */
public class DatabaseMeta {

    private String name;

    private DatabaseType databaseType;

    private String hostname;

    private int port;

    private String databaseName;

    private String user;

    private String password;

    private String owner;

    private Dialect dialect;

    public DatabaseMeta(String name, int type, String hostname, int port, String databaseName, String user, String password, String owner) {
        this.name = name;
        this.hostname = hostname;
        this.port = port;
        this.databaseName = databaseName;
        this.user = user;
        this.password = password;
        this.owner = owner;
        this.databaseType = DatabaseType.toEnum(type);

        this.getDialect();
    }

    private void getDialect() {
        switch (databaseType){
            case Oracle:
                dialect = new OracleDialect();
                break;
            case Mysql:
                dialect = new MysqlDialect();
                break;
            default:
                throw new ExtractException("Unsupported DatabaseType");
        }
    }

    public String concatToDateSql(int date) {
        return dialect.concatToDateSql(date);
    }

    public String getOwnerTableCombination(String tableName) {
        return dialect.getOwnerTableCombination(owner, tableName);
    }

    public String surroundKey(String key) {
        return dialect.surroundKey(key);
    }

    public String getURL() {
        return dialect.getURL(hostname, port, databaseName);
    }

    public Properties getConnectionProperties() {
        return dialect.getConnectionProperties();
    }

    public Properties getPoolProperties() {
        return dialect.getPoolProperties();
    }

    public String getDriverClassName() {
        return databaseType.getDriverClassName();
    }

    public String getTableColumnsSql(String tableName) {
        return dialect.getTableColumnsSql(tableName);
    }

    public void setFetchSize(Statement statement) throws SQLException {
        dialect.setFetchSize(statement);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DatabaseType getDatabaseType() {
        return databaseType;
    }

    public void setDatabaseType(DatabaseType databaseType) {
        this.databaseType = databaseType;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
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

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public void setDialect(Dialect dialect) {
        this.dialect = dialect;
    }

    @Override
    public String toString() {
        return "DatabaseMeta{" +
            "databaseType=" + databaseType +
            ", hostname='" + hostname + '\'' +
            ", port=" + port +
            ", databaseName='" + databaseName + '\'' +
            ", user='" + user + '\'' +
            ", password='" + password + '\'' +
            '}';
    }

    public String getDisplayName() {
        return hostname;
    }
}
