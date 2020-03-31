package com.ranga;

import com.ranga.constants.HDFSConstants;
import com.ranga.util.HDFSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HDFSJavaAPIDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSJavaAPIDemo.class);

    public static void main(String[] args) {

        if (args.length < 1) {
            LOGGER.error("\nUsage:\t\tHDFSJavaAPIDemo <namenode_server_host:port>\nExample:\tHDFSJavaAPIDemo hdfs://localhost:8020");
            System.err.println("Usage:\t\tHDFSJavaAPIDemo <namenode_server_host:port>\nExample:\tHDFSJavaAPIDemo hdfs://localhost:8020");
            System.exit(0);
        }

        Map<String, String> properties = new HashMap<>();
        properties.put(HDFSConstants.FS_DEFAULT_FS, args[0]);
        HDFSClient hdfsClient = new HDFSClient(properties);
        String hdfsBaseDir = "/ranga/hdfs/data/";
        String fileName = "README.md";
        String renameFileName = "README_1.md";
        String readMeFile = hdfsBaseDir + fileName;
        String renamedReadMeFile = hdfsBaseDir + renameFileName;

        LOGGER.info("Creating a hdfs dir {}", hdfsBaseDir);
        hdfsClient.createDirectory(hdfsBaseDir);

        LOGGER.info("Creating a file {} in hdfs.", readMeFile);
        String fileContent = "\nHadoop Components: \n " +
                "---------------------------------------------------"+
                "\n 1. HDFS - Hadoop Distributed File System" +
                "\n 2. MR - MAPREDUCE" +
                "\n 3. YARN - Yet Another Resource Negotiator\n";

        boolean isFileCrated = hdfsClient.writeDataToHDFS(fileContent.getBytes(), readMeFile);
        if(isFileCrated) {
            LOGGER.info("File <{}> created successfully ", readMeFile);

            LOGGER.info("Reading data from hdfs file <{}>", readMeFile);
            String hdfsFileContent = hdfsClient.readDataFromHDFS(readMeFile);
            LOGGER.info("File content {}", hdfsFileContent);

            LOGGER.info("Renaming a file");
            hdfsClient.renameFile(readMeFile, renamedReadMeFile);

            LOGGER.info("Listing file(s) under <{}> directory.", hdfsBaseDir);
            List<String> files = hdfsClient.listFiles(hdfsBaseDir);
            String filePaths = String.join("\n", files);

            LOGGER.info(filePaths);
        }

        LOGGER.info("Deleting a hdfs dir <{}>", hdfsBaseDir);
        hdfsClient.deleteDirectory(hdfsBaseDir);

    }
}
