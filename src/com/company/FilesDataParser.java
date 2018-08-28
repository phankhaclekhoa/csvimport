








package com.company;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import com.metabiota.infra.upload.processing.util.csv.DataParserCsvToBeanFilter;
import com.metabiota.infra.upload.processing.util.csv.DataParserIterableCsvToBean;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.bean.MappingStrategy;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import lombok.Setter;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FilesDataParser<T> {

    private static final Logger LOG = LogManager.getLogger(FilesDataParser.class);
    private static final String DEFAULT_CHARSET = "UTF-8";
    private final MappingStrategy<T> strategy;

    private InputStream inStream;
    private DataParserIterableCsvToBean csvToBean;

    public FilesDataParser(MappingStrategy<T> strategy) {
        this.strategy = strategy;
    }

    @Setter
    private FileDataHelper fileDataHelper;

    public Optional<Iterator<T>> getDataIterator(FilesRecord fileRecord, String s3AccessKey, String s3SecretKey, String s3Bucket, String s3Folder, String cacheFolder, Integer retries) {
        try {
            close();
            if (fileDataHelper == null) {
                fileDataHelper = new FileDataHelper();
            }
            
            inStream = fileDataHelper.retrieveFileInputStream(fileRecord, s3AccessKey, s3SecretKey, s3Bucket, s3Folder, cacheFolder, retries);
            return Optional.of(createCSVIterator(inStream));
        } catch (Exception e ){

           // return handleInputStreamError(fileRecord, e, PlatformErrorMessage.ERROR_GENERIC_MESSAGE.getMessage());
        }
    }


    private static Reader createNewReader(InputStream inputStream) throws IOException {
        BOMInputStream bomInputStream = new BOMInputStream(inputStream);
        if(bomInputStream.hasBOM()){
            return new InputStreamReader(bomInputStream, bomInputStream.getBOMCharsetName());
        } else {
            return new InputStreamReader(bomInputStream, DEFAULT_CHARSET);
        }
    }

    protected Iterator<T> createCSVIterator(InputStream inputStream) throws IOException {
        Reader reader = createNewReader(inputStream);
        CSVReaderBuilder csvBuilder = new CSVReaderBuilder(reader);
        CSVReader csvReader = csvBuilder.withFieldAsNull(CSVReaderNullFieldIndicator.BOTH).build();
        DataParserCsvToBeanFilter filter = new DataParserCsvToBeanFilter();
        csvToBean = new DataParserIterableCsvToBean(csvReader, strategy, filter);
        return csvToBean.iterator();
    }

//    private Optional<Iterator<T>> handleInputStreamError(FilesRecord file, Exception e, String errorMessage) {
//        file.setStatus(FileStatus.INVALID);
//        LOG.error(errorMessage, e);
//        fileDataHelper.updateFilesRecord(file);
//        return Optional.empty();
//    }

    public synchronized Set<String> getParseErrors(int rowNumber) {
        if (csvToBean != null) {
            Set<String> results = csvToBean.getParseErrors(rowNumber);
            return results;
        } else {
            return new HashSet<>();
        }
    }

    public void close() {
        if (inStream != null) {
            try {
                inStream.close();
            } catch (IOException e) {
                LOG.error("Error closing input stream", e);
                e.printStackTrace();
            } finally {
                inStream = null;
            }
        }
    }
}







