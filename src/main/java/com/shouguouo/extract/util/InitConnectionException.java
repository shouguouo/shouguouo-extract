package com.shouguouo.extract.util;

import java.io.Serializable;

/**
 * @author shouguouo~
 * @date 2020/9/1 - 14:15
 */
public class InitConnectionException extends Exception implements Serializable {

    private static final long serialVersionUID = 2160715822893090771L;

    public InitConnectionException(String msg) {
        super(msg);
    }

    public InitConnectionException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public InitConnectionException(Throwable cause) {
        super(cause);
    }
}
