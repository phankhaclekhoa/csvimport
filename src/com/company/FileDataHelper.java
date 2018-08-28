package com.company;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FileDataHelper {

//    @Setter
//    private FilesTable filesTable = new FilesTable();

    private static final Logger LOG = LogManager.getLogger(FileDataHelper.class);

    private static final int GZIP_BUFFER_SIZE = 100; //TODO: move this to configurable prop

//    public FilesRecord retrieveFileRecord(String fileId) throws Exception {
//        //Retrieve file record for given fileId
//        List<FilesRecord> fileRecords = filesTable.getRecordsById(fileId);
//        if (!fileRecords.isEmpty() && fileRecords.size() == 1)
//            return fileRecords.get(0);
//        else if (fileRecords.isEmpty()) {
//            LOG.warn("File with id: {} not found in files table", fileId);
//            throw new PlatformException(PlatformErrorMessage.ERROR_GENERIC_MESSAGE,
//                    "File id not found in files table: " + fileId);
//        } else {
//            throw new PlatformException(PlatformErrorMessage.ERROR_GENERIC_MESSAGE,
//                    "Multiple records associated to the same fileId in files table: " + fileId);
//        }
//    }

    public InputStream retrieveFileInputStream(FilesRecord fileRecord, String s3AccessKey, String s3SecretKey,
                                                String s3Bucket, String s3Folder, String cacheFolder, Integer retries)
            throws IOException, InterruptedException {

        return new FileInputStream("C:/Users/pklkhoa/Downloads/Life_dupRows.csv");
    }

//    public Boolean updateFilesRecord(FilesRecord file) {
//        try {
//            filesTable.addOrUpdate(file);
//            return true;
//        } catch (Exception e) {
//            LOG.error("Files table add/update failed!", e);
//            return false;
//        }
//    }
}








