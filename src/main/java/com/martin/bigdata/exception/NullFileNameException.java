package com.martin.bigdata.exception;

/**
 * @author martin
 * @desc 文件名称为空
 */
public class NullFileNameException extends RuntimeException {
    public NullFileNameException(String message) {
        super(message);
    }
}
