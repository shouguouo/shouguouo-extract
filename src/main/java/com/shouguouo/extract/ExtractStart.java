package com.shouguouo.extract;

import com.google.common.collect.Lists;
import com.shouguouo.extract.database.DatabaseMeta;
import com.shouguouo.extract.enums.ExtractType;
import com.shouguouo.extract.runner.AbstractRunner;
import com.shouguouo.extract.runner.Exchanger;
import com.shouguouo.extract.runner.ReaderRunner;
import com.shouguouo.extract.runner.WriterRunner;
import com.shouguouo.extract.util.PathUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author shouguouo~
 * @date 2020/9/30 - 11:28
 */
public class ExtractStart {

    private static Logger logger = LogManager.getLogger(ExtractStart.class);

    private static Properties properties;

    public static ExtractType type = ExtractType.ALL;

    static ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
        32,
        256,
        10L,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(1000),
        new ThreadPoolExecutor.CallerRunsPolicy());

    public static void main(String[] args) throws IOException, InterruptedException {
        logger.info("======================by shougouuo=========================================");
        File file = new File(PathUtil.getServicePath() + "/extract-config.properties");

        logger.info("Using extract config file: {}", file.getAbsolutePath());
        if (file.exists()) {
            properties = PropertiesLoaderUtils.loadProperties(new FileSystemResource(file));
        }
        logger.info("Properties: {}", properties);
        Map<String, String> paramMap = new HashMap<>();
        for (String param : params) {
            assert StringUtils.isNotBlank(properties.getProperty(param));
            paramMap.put(param, properties.getProperty(param));
        }
        List<String> tableList = Lists.newArrayList(paramMap.get("tableName").split(","));

        DatabaseMeta source = new DatabaseMeta("source", Integer.parseInt(properties.getProperty("source.type")),
            properties.getProperty("source.hostname"),
            Integer.parseInt(properties.getProperty("source.port")),
            properties.getProperty("source.databaseName"),
            properties.getProperty("source.user"),
            properties.getProperty("source.password"),
            properties.getProperty("source.user")
        );
        DatabaseMeta target = new DatabaseMeta("target", Integer.parseInt(properties.getProperty("target.type")),
            properties.getProperty("target.hostname"),
            Integer.parseInt(properties.getProperty("target.port")),
            properties.getProperty("target.databaseName"),
            properties.getProperty("target.user"),
            properties.getProperty("target.password"),
            properties.getProperty("target.user")
        );
        type = ExtractType.toEnum(Integer.parseInt(properties.getProperty("extractModel")));

        List<AbstractRunner> runnerList = new ArrayList<>();
        for (String tableName : tableList) {
            tableName = tableName.trim().toUpperCase();
            Exchanger exchanger = new Exchanger();
            ReaderRunner reader = new ReaderRunner(tableName, source, exchanger);
            WriterRunner writer = new WriterRunner(tableName, target, exchanger);
            runnerList.add(reader);
            runnerList.add(writer);
        }
        CountDownLatch latch = new CountDownLatch(runnerList.size());
        runnerList.forEach(x -> x.addLatch(latch));
        runnerList.forEach(x -> threadPool.execute(x));
        latch.await();
        logger.info("执行完毕");
    }


    private static List<String> params = Lists.newArrayList(
        "source.type",
        "source.hostname",
        "source.port",
        "source.databaseName",
        "source.user",
        "source.password",
        "target.type",
        "target.hostname",
        "target.port",
        "target.databaseName",
        "target.user",
        "target.password",
        "tableName");

}
