package com.shouguouo.extract.database;

import com.shouguouo.extract.util.InitConnectionException;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author shouguouo~
 * @date 2020/8/21 - 15:06
 */
public class ConnectionPoolUtil {

    private static Logger logger = LogManager.getLogger(ConnectionPoolUtil.class);

    private static final ConcurrentHashMap<String, HikariDataSource> dataSourceMap = new ConcurrentHashMap<>();

    private static final ReentrantLock lock = new ReentrantLock();

    public static Connection getConnection(DatabaseMeta databaseMeta) throws InitConnectionException {
        lock.lock();
        try {
            if (!isRegistered(databaseMeta)) {
                addDataSource(databaseMeta);
            }
            return dataSourceMap.get(databaseMeta.toString()).getConnection();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new InitConnectionException(e.getMessage(), e);
        } finally {
            lock.unlock();
        }
    }

    private static void addDataSource(DatabaseMeta databaseMeta) {
        HikariConfig config = new HikariConfig(databaseMeta.getPoolProperties());
        config.setJdbcUrl(databaseMeta.getURL());
        config.setDriverClassName(databaseMeta.getDriverClassName());
        config.setUsername(databaseMeta.getUser());
        config.setPassword(databaseMeta.getPassword());
        config.setPoolName("HikariPool for " + databaseMeta.getDisplayName());
        config.setAutoCommit(true);
        for (Map.Entry<Object, Object> entry : databaseMeta.getConnectionProperties().entrySet()) {
            config.addDataSourceProperty(entry.getKey().toString(), entry.getValue());
        }

        HikariDataSource dataSource = new HikariDataSource(config);

        dataSourceMap.put(databaseMeta.toString(), dataSource);
    }

    private static boolean isRegistered(DatabaseMeta databaseMeta) {
        return dataSourceMap.containsKey(databaseMeta.toString());
    }

}
