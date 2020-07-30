package com.martin.bigdata.controller;

import com.martin.bigdata.exception.FileNotExistException;
import com.martin.bigdata.exception.NullFileNameException;
import com.martin.bigdata.util.HdfsUtil;
import com.martin.bigdata.util.WebUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.OutputStream;

/**
 * @author martin
 * @desc 用于读写 hdfs 的接口
 */
@Controller
@RequestMapping("/hdfs")
public class HdfsController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource(name = "haHdfs")
    FileSystem hdfs;

    @Value("${hdfs.changjingyun-directory}")
    private String changJingYunDirectory;

    /**
     * 使用 GET 请求方式下载，请求参数为 fileName
     */
    @GetMapping(value = "/download")
    public void downloadFromHDFS(HttpServletRequest request, HttpServletResponse response) throws Exception {

        String fileName = request.getParameter("fileName");
        String filePath = changJingYunDirectory + fileName;

        WebUtil.setResponseForDownload(response,fileName);

        if (fileName == null) {
            WebUtil.setResponseForError(response);
            throw new NullFileNameException("请求参数传入的文件名为空！");
        }

        if (!hdfs.exists(new Path(filePath))) {
            WebUtil.setResponseForError(response);
            throw new FileNotExistException("hdfs 上不存在指定文件！");
        }

        // 下载文件
        OutputStream os = response.getOutputStream();
        HdfsUtil.getFileOutputStream(hdfs, filePath, os);

        logger.info(fileName + " 下载成功!");

    }

}
