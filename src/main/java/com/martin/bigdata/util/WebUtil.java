package com.martin.bigdata.util;

import javax.servlet.http.HttpServletResponse;

/**
 * @author martin
 */
public class WebUtil {

    public static void setResponseForDownload(HttpServletResponse response,String fileName){
        //设置响应头
        response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
        response.setHeader("Pragma", "no-cache");
        response.setHeader("Expires", "0");
        response.setHeader("charset", "utf-8");
        //force not to open downloaded file
        response.setContentType("application/force-download");
        response.setHeader("Content-Transfer-Encoding", "binary");
        response.setHeader("Content-Disposition", "attachment;filename=\"" + fileName + "\"");
    }


    public static void setResponseForError(HttpServletResponse response){
        response.setContentType("application/json");
    }
}
