package com.ranga.util;

import com.ranga.exception.HDFSFileException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

public class HDFSFileUtil implements Serializable, Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSClient.class);
    protected final FileSystem fileSystem;

    public HDFSFileUtil(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    public boolean deleteDirectory(String directory) {
        Path hdfsFilePath = new Path(directory);
        return deleteDirectory(hdfsFilePath, true);
    }

    public boolean deleteDirectory(Path path) {
        return deleteDirectory(path, true);
    }

    public boolean deleteDirectory(Path hdfsFilePath, boolean recursive) {
        try {
            if (fileSystem.exists(hdfsFilePath)) {
                fileSystem.delete(hdfsFilePath, recursive);
            }
            LOGGER.info("HDFS directory <{}> deleted successfully.", hdfsFilePath);
        } catch (Exception e) {
            LOGGER.error("Encountered exception while deleting a directory {}", hdfsFilePath, e);
            throw new HDFSFileException(e);
        }
        return true;
    }

    public boolean createDirectory(String directory) {
        Path path = new Path(directory);
        return createDirectory(path);
    }

    public boolean createDirectory(Path hdfsFilePath) {
        try {
            if (!fileSystem.exists(hdfsFilePath)) {
                fileSystem.mkdirs(hdfsFilePath);
                LOGGER.info("HDFS directory <{}> created successfully.", hdfsFilePath);
            }
        } catch (Exception e) {
            LOGGER.error("Encountered exception while creating a directory {}", hdfsFilePath, e);
            throw new HDFSFileException(e);
        }
        return true;
    }

    public boolean rename(String oldPath, String newPath) {
        try {
            Path old = new Path(oldPath);
            Path newP = new Path(newPath);
            fileSystem.rename(old, newP);
            LOGGER.info("HDFS File {} is successfully renamed to {}", oldPath, newPath);
        } catch (Exception e) {
            LOGGER.error("Encountered exception while renaming a file {} with new file {}. ", oldPath, newPath, e);
            throw new HDFSFileException(e);
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        closeFileSystem();
    }

    public void closeFileSystem() throws IOException {
        if (fileSystem != null) {
            fileSystem.close();
        }
    }
}