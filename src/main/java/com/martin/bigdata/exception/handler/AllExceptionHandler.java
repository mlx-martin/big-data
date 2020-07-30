package com.martin.bigdata.exception.handler;

import com.martin.bigdata.constant.ResponseConstant;
import com.martin.bigdata.pojo.model.ResponseBodyVO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Arrays;

/**
 * @author martin
 * @desc 全局异常处理
 */
@RestControllerAdvice
public class AllExceptionHandler {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @ExceptionHandler(Exception.class)
    public ResponseBodyVO<String> handleException(Exception e) {
        ResponseBodyVO<String> responseBodyVO = new ResponseBodyVO<>(ResponseConstant.FAIL_STATUS, ResponseConstant.SERVER_ERROR_CODE, e.getMessage(),null);
        logger.error(e.getMessage());
        logger.error(Arrays.toString(e.getStackTrace()));
        return responseBodyVO;
    }

}
