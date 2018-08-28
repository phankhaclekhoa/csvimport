package com.company;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FileUploadProcessor {

    private static Logger LOG = LogManager.getLogger(FileUploadProcessor.class);

    public static void processFile(FileDataType fileType, Boolean processState) throws IOException, ClassNotFoundException {
        LOG.info("Processing file type: " + fileType);
        AbstractProcessor fileProcessor = getFileProcessorByFileType(fileType, processState);
        if (fileProcessor != null) {
            fileProcessor.process(filesRecord);
        } else {
            LOG.error("No known processor for file type: " + fileType);
        }
    }

    public static AbstractProcessor getFileProcessorByFileType(FileDataType fileType, Boolean processState) throws IOException, ClassNotFoundException {
        AbstractProcessor fileProcessor;
        switch (fileType) {
            case CUSTOMER:
                fileProcessor = new CustomerProcessor();
                break;
            default:
                fileProcessor = null;
                break;
        }

        return fileProcessor;
    }
}
