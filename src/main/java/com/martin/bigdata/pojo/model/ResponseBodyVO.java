package com.martin.bigdata.pojo.model;

/**
 * @author martin
 */
public class ResponseBodyVO<T> {

    private String status;
    private String code;
    private String message;
    private T info;

    public ResponseBodyVO(String status, String code, String message, T info) {
        this.status = status;
        this.code = code;
        this.message = message;
        this.info = info;
    }

    @Override
    public String toString() {
        return "JsonResultVO{" +
                "status='" + status + '\'' +
                ", code='" + code + '\'' +
                ", message='" + message + '\'' +
                ", info=" + info +
                '}';
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public T getInfo() {
        return info;
    }

    public void setInfo(T info) {
        this.info = info;
    }
}
