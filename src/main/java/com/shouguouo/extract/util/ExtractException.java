package com.shouguouo.extract.util;

import java.io.Serializable;

/**
 * @author shouguouo~
 * @date 2020/9/1 - 14:42
 */
public class ExtractException extends RuntimeException implements Serializable {
    private static final long serialVersionUID = 2447219525172976133L;

    public ExtractException(String msg) {
        super(msg);
    }

    public ExtractException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public ExtractException(Throwable cause) {
        super(cause);
    }
}
