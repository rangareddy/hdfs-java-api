package com.ranga.util;

import com.ranga.exception.HDFSFileException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/*
 *  HDFSFileReader Utility used to read the data from HDFS
 *  @author Ranga Reddy
 *  @version 1.0
 *  @since 31/3/2020
 */

public class HDFSFileReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSFileReader.class);
    private final FileSystem fileSystem;

    public HDFSFileReader(FileSystem fs) {
        this.fileSystem = fs;
    }

    public String readFileDataFromHDFS(String filePath) {
        return readFileDataFromHDFS(new Path(filePath));
    }

    public String readFileDataFromHDFS(final Path path) {
        validateFilePath(path);
        InputStream stream = null;
        try {
            stream = fileSystem.open(path);
            StringWriter writer = new StringWriter();
            IOUtils.copy(stream, writer, StandardCharsets.UTF_8);
            return writer.toString();
        } catch (Exception ex) {
            LOGGER.error("Exception occurred while getting the file from {}", path, ex);
            throw new HDFSFileException(ex);
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    LOGGER.error("Exception occurred while closing stream.", e);
                }
            }
        }
    }

    public List<String> listFiles(String filePath) {
        List<String> fileNames = new ArrayList<>();
        try {
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path(filePath), true);
            while (iterator.hasNext()) {
                LocatedFileStatus next = iterator.next();
                String fileName = next.getPath().toString();
                fileNames.add(fileName);
            }
        } catch (Exception ex) {
            LOGGER.error("Exception occurred while listing files from the path <{}>", filePath, ex);
            throw new HDFSFileException(ex);
        }
        return fileNames;
    }

    public void validateFilePath(Path path) {
        LOGGER.debug("Reading the file from hdfs. HDFS path is <{}>", path.getName());
        try {
            if (!fileSystem.exists(path)) {
                String filePath = new File(path.toUri()).getAbsolutePath();
                LOGGER.error("File <{}> does not exist." + filePath);
                throw new HDFSFileException("File <" + filePath + "> does not exist.");
            }
        } catch (Exception ex) {
            throw new HDFSFileException(ex);
        }
    }
}