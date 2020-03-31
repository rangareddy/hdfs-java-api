package com.ranga.util;

import com.ranga.exception.HDFSFileException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

/*
 *  HDFSFileWriter Utility used to write the data to HDFS
 *  @author Ranga Reddy
 *  @version 1.0
 *  @since 31/3/2020
 */

public class HDFSFileWriter extends HDFSFileUtil {

    private static final Logger logger = LoggerFactory.getLogger(HDFSFileWriter.class);

    public HDFSFileWriter(FileSystem fs) {
        super(fs);
    }

    protected boolean writeFileToHDFS(Path localFilePath, Path hdfsFilePath) {
        logger.debug("Copying file from localPath <{}> to hdfsPath <{}>", localFilePath, hdfsFilePath);
        try {
            createDirectory(hdfsFilePath);
            fileSystem.copyFromLocalFile(localFilePath, hdfsFilePath);
            logger.info("File is successfully copied from <{}> to <{}>", localFilePath, hdfsFilePath);
            return true;
        } catch (Exception e) {
            logger.error("Exception occurred while writing data from localPath {} to hdfsPath {}.", localFilePath, hdfsFilePath, e);
            throw new HDFSFileException(e);
        }
    }

    public boolean writeContentToHDFS(byte[] fileContent, String filePath) {
        return writeToHDFS(fileContent, new Path(filePath));
    }

    public String writeFileToHDFS(String filePath, String hdfsDir) {
        logger.debug("Copying file from <{}> to hdfs <{}>", filePath, hdfsDir);
        String hdfsFilePath = null;
        try {
            boolean isWritten = writeFileToHDFS(new Path(filePath), new Path(hdfsDir));
            if (isWritten) {
                if (hdfsDir.startsWith("hdfs:")) {
                    hdfsFilePath = hdfsDir + "/" + new File(filePath).getName();
                } else {
                    // hdfsFilePath = properties.get(HadoopConstants.HDFS_FILE_SYSTEM_URI) + hdfsDir + "/" + new File(filePath).getName();
                }
            }
        } catch (Exception e) {
            logger.error("Encountered exception while writing file from local to hdfs. File path " + filePath + ", HDFs dir " + hdfsDir, e);
            throw new HDFSFileException(e);
        }
        return hdfsFilePath;
    }

    public boolean writeToHDFS(byte[] fileContent, String hdfsFilePath) {
        return writeToHDFS(fileContent, new Path(hdfsFilePath));
    }

    public boolean writeToHDFS(byte[] fileContent, Path hdfsFilePath) {
        try {
            deleteDirectory(hdfsFilePath);
            OutputStream os = fileSystem.create(hdfsFilePath);
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8));
            br.write(new String(fileContent, StandardCharsets.UTF_8));
            br.close();

            logger.info("Data is successfully written to hdfsPath <{}>", hdfsFilePath);
            return true;
        } catch (Exception e) {
            logger.error("Exception occurred while writing data to hdfsPath <{}>.", hdfsFilePath, e);
            throw new HDFSFileException(e);
        }
    }
}