package com.ranga.util;

import com.ranga.exception.HDFSClientException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/*
 *  Used to build the HDFS Client
 *  @author Ranga Reddy
 *  @version 1.0
 *  @since 31/3/2020
 */

public class HDFSClient implements Serializable {

    private final HDFSFileReader hdfsFileReader;
    private final HDFSFileWriter hdfsFileWriter;

    public HDFSClient(Map<String, String> conf) {
        Configuration configuration = ConfigUtils.createConf(conf);
        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            hdfsFileReader = new HDFSFileReader(fileSystem);
            hdfsFileWriter = new HDFSFileWriter(fileSystem);
        } catch (Exception ex) {
            throw new HDFSClientException("Exception occurred while getting the FileSystem object.", ex);
        }
    }

    public String readDataFromHDFS(String hdfsFilePath) {
        return hdfsFileReader.readFileDataFromHDFS(hdfsFilePath);
    }

    public boolean writeFileToHDFS(Path localFilePath, Path hdfsFilePath) {
        return hdfsFileWriter.writeFileToHDFS(localFilePath, hdfsFilePath);
    }

    public String writeFileToHDFS(String localFilePath, String hdfsFilePath) {
        return hdfsFileWriter.writeFileToHDFS(localFilePath, hdfsFilePath);
    }

    public List<String> listFiles(String filePath) {
        return hdfsFileReader.listFiles(filePath);
    }

    public boolean writeDataToHDFS(byte[] fileContent, String hdfsFilePath) {
        return hdfsFileWriter.writeToHDFS(fileContent, hdfsFilePath);
    }

    public boolean createDirectory(String hdfsDirectory) {
        return hdfsFileWriter.createDirectory(hdfsDirectory);
    }

    public boolean deleteDirectory(String hdfsDirectory) {
        return hdfsFileWriter.deleteDirectory(hdfsDirectory);
    }

    public boolean renameFile(String oldFileName, String newFileName) {
        return hdfsFileWriter.rename(oldFileName, newFileName);
    }
}