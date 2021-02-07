package com.shouguouo.extract.runner;

import com.shouguouo.extract.database.DatabaseMeta;
import com.shouguouo.extract.util.DatabaseUtil;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author shouguouo~
 * @date 2020/8/26 - 16:05
 */
public abstract class AbstractRunner implements Runnable {

    protected final String tableName;

    protected final String tableNameWithOwner;

    protected final DatabaseMeta databaseMeta;

    protected final Exchanger exchanger;

    protected CountDownLatch latch;

    public AbstractRunner(String tableName, DatabaseMeta databaseMeta, Exchanger exchanger) {
        this.tableName = tableName;
        this.tableNameWithOwner = databaseMeta.getOwnerTableCombination(tableName);
        this.databaseMeta = databaseMeta;
        this.exchanger = exchanger;
    }

    protected List<String> getTableColumns(Connection connection) {
        return DatabaseUtil.getTableColumnsByConn(connection, tableNameWithOwner);
    }

    public void addLatch(CountDownLatch latch) {
        this.latch = latch;
    }
}
