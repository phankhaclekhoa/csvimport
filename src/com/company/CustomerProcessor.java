package com.company;

import com.google.common.collect.Lists;
import com.metabiota.hbase.record.FilesRecord;
import com.metabiota.hbase.record.GenericDataRowsRecord;
import com.metabiota.infra.upload.processing.processor.aggregation.GenericDataAggregation;
import com.metabiota.infra.upload.processing.processor.derivation.GeoLocationDerivation;
import com.metabiota.infra.upload.processing.processor.metrics.UserFileDataMetrics;
import com.metabiota.infra.upload.processing.processor.validation.GenericDataRowsValidation;
import com.metabiota.infra.upload.processing.processor.validation.GenericRequiredFieldsValidation;
import com.metabiota.infra.upload.processing.util.FileProcessorUtil.CsvFields;
import com.metabiota.infra.upload.processing.util.FileProcessorUtil.DtoFields;
import com.metabiota.infra.upload.processing.util.GenericDataHelper;
import com.metabiota.infra.upload.processing.util.UploadProcessingConfig;
import com.metabiota.pojo.platform.files.ProcessingOptions;
import com.opencsv.bean.MappingStrategy;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by nhchon on 10/23/2017 4:46 PM.
 */
public class CustomerProcessor extends AbstractUserFileProcessor<GenericDataRowsRecord> {

    private static final Logger LOG = LogManager.getLogger(CustomerProcessor.class);
    private ConcurrentHashMap<String, Integer> genericDataRecordsIds = new ConcurrentHashMap();
    private List<GenericDataRowsRecord> genericDataRecords = Collections.synchronizedList(Lists.newArrayList());

    @Override
    protected Map<String, String> getColumnMappings() {
        Map<String, String> columnMap = new HashMap<>();
        columnMap.put(CsvFields.GROUP_TITLE, DtoFields.GROUPING);
        columnMap.put(CsvFields.UNITS, DtoFields.UNITS);
        columnMap.put(CsvFields.STREET_ADDRESS, DtoFields.STREET_ADDRESS);
        columnMap.put(CsvFields.CITY, DtoFields.CITY);
        columnMap.put(CsvFields.STATE, DtoFields.STATE);
        columnMap.put(CsvFields.ZIP, DtoFields.ZIP);
        columnMap.put(CsvFields.COUNTRY, DtoFields.COUNTRY_CODE);
        columnMap.put(CsvFields.LATITUDE, DtoFields.LATITUDE);
        columnMap.put(CsvFields.LONGITUDE, DtoFields.LONGITUDE);
        return columnMap;
    }

    @Override
    public MappingStrategy getMappingStrategy() {
        return getMappingStrategy(GenericDataRowsRecord.class);
    }

    @Override
    protected Boolean init(FilesRecord record, MappingStrategy<GenericDataRowsRecord> mappingStrategy) {
        return super.init(record, mappingStrategy);
    }

    /**
     * Main function
     * Process rows and do aggregation
     *
     * @param filesRecord the file record from HBase that needs processing
     */
    @Override
    public void processRecords(FilesRecord filesRecord) {
        long startTime = System.nanoTime();
        // Start processing
        UserFileDataMetrics metrics = new UserFileDataMetrics();
        Iterator<GenericDataRowsRecord> dataIterator = getDataIterator();
        GenericDataAggregation aggregator = new GenericDataAggregation();
        AtomicInteger rowCount = new AtomicInteger(0);
        toStream(dataIterator, false)
                .map(record -> GenericDataHelper.buildGenericDataRecord(record, filesRecord, rowCount.incrementAndGet()))
                .parallel()
                .forEach(nr -> {
                    metrics.addToTotalRecords(1);
                    // Add parse errors
                    Set<String> parseErrors = fileParser.getParseErrors(nr.getRowNum());
                    for (String error : parseErrors) {
                        metrics.addInvalidValueErrorMessage("InvalidValueValidation", nr.getRecord().getId(), nr.getRowNum(), error);
                    }
                    boolean duplicateRecord = isDuplicate(nr, metrics);
                    if (!duplicateRecord) {
                        processRecord(nr.getRecord(), metrics, getProcessingOptions(filesRecord), aggregator);
                        writeToHbase(rowCount);
                    }
                });
        //for remaining records (< batch size = 2000)
        GenericDataHelper.addOrUpdateGenericDataRecords(genericDataRecords);
        LOG.info("Processing result for file " + filesRecord.getId() + ": \n" + metrics.toLogOutput());
        try {
            runGenericAggregationJob(aggregator, filesRecord.getId());
        } catch (Exception e) {
            LOG.error("Error aggregating generic data records", e);
            System.exit(-1);
        }

        // Update status file after running aggregation successful
        try {
            String uploadSummary = new ObjectMapper().writeValueAsString(metrics.createUserFileDataReport());
            LOG.info("Upload Summary: " + uploadSummary);
            filesRecord.setUploadSummary(uploadSummary);
            if (!fileDataHelper.updateFilesRecord(filesRecord)) {
                LOG.error("Failed to update upload summary for file record with fileID: " + filesRecord.getId());
                closeFileParserAndExit();
            }
            long endTime = System.nanoTime();
            LOG.info("Finished all processing for file: " + filesRecord.getId() + " in " + TimeUnit.SECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS) + " seconds ");
        } catch (IOException e) {
            LOG.error("Unable to create generic data report upload summary", e);
            closeFileParserAndExit();
        }
        fileParser.close();
    }

    private void processRecord(GenericDataRowsRecord record, UserFileDataMetrics metrics, ProcessingOptions processingOptions,
                               GenericDataAggregation aggregator) {
        boolean validated = validateRecord(record, metrics);

        if (validated && processingOptions.getGeocodingEnabled()) {
            geocodeRecord(record, metrics, aggregator);
        }
    }

    /**
     * Write data into database which they are geocoded successully.
     *
     * @param record
     * @param metrics
     * @param aggregator
     */
    private void geocodeRecord(GenericDataRowsRecord record, UserFileDataMetrics metrics,
                               GenericDataAggregation aggregator) {
        Boolean geocoded = applyGeocodingRules(record, metrics);
        if (geocoded) {
            aggregateRecord(record, aggregator);

            genericDataRecords.add(record);
            metrics.addSuccessfulProcessedRecords(1);
        } else {
            metrics.addFailedProcessingRecords(1);
        }
    }

    private void aggregateRecord(GenericDataRowsRecord record, GenericDataAggregation aggregator) {
        aggregator.add(record);
    }

    private boolean validateRecord(GenericDataRowsRecord record, UserFileDataMetrics metrics) {
        Boolean validated = applyValidationRules(record, metrics);
        if (validated) {
            metrics.addToSuccessfulValidatedRecords(1);
        } else {
            metrics.addToFailedValidationRecords(1);
        }
        return validated;
    }


    /**
     * Do aggregation
     *
     * @param aggregator
     * @throws Exception
     */
    private void runGenericAggregationJob(GenericDataAggregation aggregator, String userId) throws Exception {
        Boolean jobRunStatus = GenericDataHelper.aggregate(aggregator, userId);
        LOG.info("Generic data aggregation result: " + jobRunStatus);
    }

    private Boolean applyGeocodingRules(GenericDataRowsRecord record, UserFileDataMetrics metrics) {
        Boolean result = false;
        try {
            result = new GeoLocationDerivation(record, metrics).derivationSuccessful();
        } catch (SolrServerException ssex) {
            // if the Solr geocoding service is not available, I think the processing of the file should error out rather than continuing.
            // We should expect that the Solr service is highly available and that the file is geocoded properly if it has completed.
            LOG.error("Something wrong with Solr Server. Failed to retrieve geocoding from it.", ssex);
            closeFileParserAndExit();
        }

        return result;
    }

    private Boolean applyValidationRules(GenericDataRowsRecord record, UserFileDataMetrics metrics) {
        Boolean requiredFieldsValidation = new GenericRequiredFieldsValidation(record, metrics).validationSuccessful();
        Boolean unitsValidation = new GenericDataRowsValidation(record, metrics).validationSuccessful();

        return unitsValidation && requiredFieldsValidation;
    }

    private boolean isDuplicate(NumberedDataRecord<GenericDataRowsRecord> record, UserFileDataMetrics metrics) {
        // Check for duplicate records
        Integer dupeIndex = GenericDataHelper.addIdWithIndex(genericDataRecordsIds, record);
        if (dupeIndex != null) {
            Integer rowNumber = GenericDataHelper.getRecordRowNumberAndId(record.getRecord().getId()).getRowNumber();
            metrics.addDuplicateRecordsErrorMessage("DuplicateRowsValidation", record.getRecord().getId(), rowNumber,
                    dupeIndex);
            metrics.addToDuplicateRecords(1);
            return true;
        }
        return false;
    }

    /**
     * Write data in batch size.
     *
     * @param rowCount
     */
    private void writeToHbase(AtomicInteger rowCount) {
        synchronized(genericDataRecords) {
            if (genericDataRecords.size() == UploadProcessingConfig.getGenericProcessorBatchSize()) {
                LOG.info("Writing to Hbase from index " + (rowCount.get() - UploadProcessingConfig.getGenericProcessorBatchSize()) + ". Number of records to write: " + UploadProcessingConfig.getGenericProcessorBatchSize());
                GenericDataHelper.addOrUpdateGenericDataRecords(genericDataRecords);
                genericDataRecords.clear();
                LOG.info("Finished writing to Hbase");
            }
        }
    }

}