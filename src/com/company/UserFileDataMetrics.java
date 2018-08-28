package com.company;


import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.text.WordUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
public class UserFileDataMetrics extends MetricsContext {
    private final int MAX_ERROR_REPORT_MESSAGES = 5000;

    private Integer totalRecords = 0;
    private Integer duplicateRecords = 0;
    private Integer failedValidationRecords = 0;
    private Integer failedProcessingRecords = 0;
    private Integer successfulValidatedRecords = 0;
    private Integer successfulProcessedRecords = 0;


    public String toLogOutput() {
        return "Metadata \n" + "Total records: " + totalRecords + "\n" +
                "Duplicate records: " + duplicateRecords + "\n" +
                "Successful validated records: " + successfulValidatedRecords + "\n" +
                "Failed validation records: " + failedValidationRecords + "\n" +
                "Successful processed records: " + successfulProcessedRecords + "\n" +
                "Failed processing records: " + failedProcessingRecords + "\n";
    }

    public void addInvalidValueErrorMessage(String ruleInfo, String contextInfo, int rowNumber, String columnName) {
        addRecord(MetricsContext.MetricsRecordType.VALIDATION_FAILURE,
                ruleInfo,
                contextInfo,
                rowNumber,
                columnName,
                rowNumber + ", Invalid " + WordUtils.capitalize(columnName));
    }

    public void addMissingValueErrorMessage(String ruleInfo, String contextInfo, int rowNumber, String columnName) {
        addRecord(MetricsContext.MetricsRecordType.VALIDATION_FAILURE,
                ruleInfo,
                contextInfo,
                rowNumber,
                columnName,
                rowNumber + ", Missing " + columnName);
    }

    public void addGeoCodeValidationErrorMessage(String ruleInfo, String contextInfo, Integer rowNumber) {
        addRecord(MetricsContext.MetricsRecordType.VALIDATION_FAILURE,
                ruleInfo,
                contextInfo,
                rowNumber,
                "",
                rowNumber.toString() + ", Geocode data validation failed");
    }

    public UserFileDataReport createUserFileDataReport() {
        UserFileDataReport report = new UserFileDataReport();
        report.setTotalRows(totalRecords);
        report.setImportedRows(totalRecords - duplicateRecords - failedValidationRecords);
        report.setProcessingErrors(failedValidationRecords + duplicateRecords);
        report.setGeocodedErrors(failedProcessingRecords);
        report.setGeocodedRows(successfulProcessedRecords);
        List<MetricsRecord> errors = getRecords().get(MetricsContext.MetricsRecordType.VALIDATION_FAILURE);
        int limit = Math.min(errors.size(), MAX_ERROR_REPORT_MESSAGES);
        Map<Integer, List<String>> errorMsgMaps = new HashedMap();
        List<String> stringList = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
            int rowNumber = errors.get(i).getRowNumber();
            String message = errors.get(i).getMessage();
            if (errorMsgMaps.containsKey(rowNumber)) {
                stringList = errorMsgMaps.get(rowNumber);
            } else {
                stringList = new ArrayList<>();
            }
            stringList.add(message.substring(message.indexOf(", ") + 2, message.length()));
            errorMsgMaps.put(rowNumber, stringList);
        }

        errorMsgMaps.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(
                e -> e.getKey() + ", " + e.getValue().stream().collect(Collectors.joining(", ")))
                .forEach(r -> {
                    report.addErrorMessage(r);
                });
        if (errors.size() > MAX_ERROR_REPORT_MESSAGES) {
            report.addErrorMessage("Error messages list was to large, limited to first: " + MAX_ERROR_REPORT_MESSAGES + " errors");
        }
        return report;
    }

    public void addDuplicateRecordsErrorMessage(String ruleInfo, String contextInfo, Integer rowNumber, Integer originalRowNumber) {
        addRecord(MetricsContext.MetricsRecordType.VALIDATION_FAILURE,
                ruleInfo,
                contextInfo,
                rowNumber,
                "",
                rowNumber + ", Duplicate Row(Row " + originalRowNumber + ")");
    }

    public void addToTotalRecords(Integer rowCount) {
        synchronized (totalRecords) {
            totalRecords += rowCount;
        }
    }

    public void addToDuplicateRecords(Integer duplicateCount) {
        synchronized (duplicateRecords) {
            duplicateRecords += duplicateCount;
        }
    }

    public void addToSuccessfulValidatedRecords(int records) {
        synchronized (successfulValidatedRecords) {
            successfulValidatedRecords += records;
        }
    }

    public void addToFailedValidationRecords(int records) {
        synchronized (failedValidationRecords) {
            failedValidationRecords += records;
        }
    }

    public void addSuccessfulProcessedRecords(int records) {
        synchronized (successfulProcessedRecords) {
            successfulProcessedRecords += records;
        }
    }

    public void addFailedProcessingRecords(int records) {
        synchronized (failedProcessingRecords) {
            failedProcessingRecords += records;
        }
    }
}
