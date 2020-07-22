package com.martin.bigdata.exception.handler;

import com.martin.bigdata.constant.ResponseConstant;
import com.martin.bigdata.exception.FileNotExistException;
import com.martin.bigdata.exception.NullFileNameException;
import com.martin.bigdata.pojo.model.ResponseVO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import javax.servlet.http.HttpServletResponse;

/**
 * @author martin
 * @desc 全局异常处理
 */
@RestControllerAdvice
public class AllExceptionHandler {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @ExceptionHandler(NullFileNameException.class)
    public ResponseVO handleNullFileNameException(NullFileNameException e,HttpServletResponse response) {
        ResponseVO responseVO = new ResponseVO(ResponseConstant.FAIL_STATUS,ResponseConstant.BAD_REQUEST_CODE,e.getMessage());
        logger.error(e.getMessage());
        logger.error(String.valueOf(e.getStackTrace()));
        return responseVO;
    }

    @ExceptionHandler(FileNotExistException.class)
    public ResponseVO handleFileNotExistException(FileNotExistException e, HttpServletResponse response ) {
        ResponseVO responseVO = new ResponseVO(ResponseConstant.FAIL_STATUS, ResponseConstant.SERVER_ERROR_CODE, e.getMessage());
        logger.error(e.getMessage());
        logger.error(String.valueOf(e.getStackTrace()));
        return responseVO;
    }

}
