package com.shouguouo.extract.database.dialect;

import com.shouguouo.extract.enums.DatabaseType;
import com.shouguouo.extract.util.PathUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;


/**
 * @author shouguouo~
 * @date 2020/8/21 - 16:36
 */
public abstract class AbstractDialect implements Dialect {

    private static Logger logger = LogManager.getLogger(AbstractDialect.class);

    protected static final Map<String, Properties> propertiesMap = new HashMap<>();

    static {
        try {
            File file = new File(PathUtil.getServicePath() + "/connection");
            if (file.exists()) {
                for (File single : Objects.requireNonNull(file.listFiles())) {
                    propertiesMap.put(single.getName(), PropertiesLoaderUtils.loadProperties(new FileSystemResource(single)));
                }
            }
        } catch (Exception e) {
            logger.error("error loading connection properties", e);
        }
    }


    @Override
    public Properties getConnectionProperties() {
        return null;
    }

    @Override
    public Properties getPoolProperties() {
        return propertiesMap.get(DatabaseType.Common.getPropertiesFileName());
    }

    @Override
    public String getUnlockTableSql() {
        return null;
    }
}
