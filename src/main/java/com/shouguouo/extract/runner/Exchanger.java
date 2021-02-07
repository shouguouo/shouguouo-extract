package com.shouguouo.extract.runner;


import com.shouguouo.extract.entity.Record;
import com.shouguouo.extract.entity.TerminatedRecord;
import com.shouguouo.extract.util.ExtractException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author shouguouo~
 * @date 2020/8/26 - 18:04
 */
public class Exchanger {

    private BlockingQueue<Record> recordQueue = new LinkedBlockingQueue<>(10000);

    private volatile AtomicBoolean shutdown = new AtomicBoolean(false);

    public void put(Record record) throws InterruptedException {
        if (shutdown.get()) {
            throw new ExtractException("already shutdown");
        }
        recordQueue.put(record);
    }

    public Record take() throws InterruptedException {
        if (shutdown.get()) {
            return TerminatedRecord.get();
        }
        return recordQueue.take();
    }

    void updateDetailLog(String errorMsg) {
        if (shutdown.compareAndSet(Boolean.FALSE, Boolean.TRUE)) {
            // 避免put阻塞，而无法终止reader线程
            recordQueue.clear();
            try {
                // 避免take阻塞，而无法终止writer线程
                recordQueue.put(TerminatedRecord.get());
            } catch (InterruptedException ignore) {
            }
        }
    }

}
