package com.martin.bigdata.exception;

/**
 * @author martin
 * @desc 文件不存在
 */
public class FileNotExistException extends RuntimeException{
    public FileNotExistException(String message) {
        super(message);
    }
}
