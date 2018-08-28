/**
 * 
 */

package com.company;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.logging.log4j.Logger;

/**
 * @author pklkhoa
 *
 */
public class MetricsContext {
	private final Date createdTimestamp = new Date();
	private final Map<MetricsContext.MetricsRecordType, List<MetricsContext.MetricsRecord>> records = new LinkedHashMap();

	public MetricsContext() {
		MetricsContext.MetricsRecordType[] var1 = MetricsContext.MetricsRecordType
				.values();
		int var2 = var1.length;

		for (int var3 = 0; var3 < var2; ++var3) {
			MetricsContext.MetricsRecordType recordType = var1[var3];
			this.records.put(recordType, new ArrayList());
		}

	}

	public void addRecord(MetricsContext.MetricsRecordType recordType,
			String ruleInfo, String contextInfo, String message) {
		MetricsContext.MetricsRecord record = new MetricsContext.MetricsRecord();
		record.setContextInfo(contextInfo);
		record.setRecordType(recordType);
		record.setMessage(message);
		record.setSourceRuleInfo(ruleInfo);
		((List) this.records.get(recordType)).add(record);
	}

	public void addRecord(MetricsContext.MetricsRecordType recordType,
			String ruleInfo, String contextInfo, int rowNumber,
			String columnName, String message) {
		MetricsContext.MetricsRecord record = new MetricsContext.MetricsRecord();
		record.setContextInfo(contextInfo);
		record.setRecordType(recordType);
		record.setSourceRuleInfo(ruleInfo);
		record.setRowNumber(rowNumber);
		record.setColumnName(columnName);
		record.setMessage(message);
		((List) this.records.get(recordType)).add(record);
	}

	public Date getCreatedTimestamp() {
		return this.createdTimestamp;
	}

	public Map<MetricsContext.MetricsRecordType, List<MetricsContext.MetricsRecord>> getRecords() {
		return this.records;
	}

	public boolean validationFailure() {
		return !((List) this.records
				.get(MetricsContext.MetricsRecordType.VALIDATION_FAILURE))
				.isEmpty();
	}

	public boolean executionFailure() {
		return !((List) this.records
				.get(MetricsContext.MetricsRecordType.EXECUTION_FAILURE))
				.isEmpty();
	}

	public boolean warnings() {
		return !((List) this.records
				.get(MetricsContext.MetricsRecordType.WARNING)).isEmpty();
	}

	public void dumpMetrics(Logger logger) {
		logger.info("Items successfully processed: "
				+ ((List) this.records
						.get(MetricsContext.MetricsRecordType.SUCCESS)).size());
		logger.info("Processing failures: "
				+ ((List) this.records
						.get(MetricsContext.MetricsRecordType.EXECUTION_FAILURE))
						.size());
		logger.info("Validation failures: "
				+ ((List) this.records
						.get(MetricsContext.MetricsRecordType.VALIDATION_FAILURE))
						.size());
		logger.info("Warnings: "
				+ ((List) this.records
						.get(MetricsContext.MetricsRecordType.WARNING)).size());
		StringBuilder sb;
		Iterator var3;
		MetricsContext.MetricsRecord record;
		if (this.executionFailure()) {
			sb = new StringBuilder();
			sb.append("=== Execution Failure Details\n");
			var3 = ((List) this.records
					.get(MetricsContext.MetricsRecordType.EXECUTION_FAILURE))
					.iterator();

			while (var3.hasNext()) {
				record = (MetricsContext.MetricsRecord) var3.next();
				sb.append(record.toString() + "\n");
			}

			logger.error(sb.toString());
		} else if (this.validationFailure()) {
			sb = new StringBuilder();
			sb.append("=== Validation Failure Details\n");
			var3 = ((List) this.records
					.get(MetricsContext.MetricsRecordType.VALIDATION_FAILURE))
					.iterator();

			while (var3.hasNext()) {
				record = (MetricsContext.MetricsRecord) var3.next();
				sb.append(record.toString() + "\n");
			}

			logger.warn(sb.toString());
		} else if (this.warnings()) {
			sb = new StringBuilder();
			sb.append("=== Warnings thrown\n");
			var3 = ((List) this.records
					.get(MetricsContext.MetricsRecordType.WARNING)).iterator();

			while (var3.hasNext()) {
				record = (MetricsContext.MetricsRecord) var3.next();
				sb.append(record.toString() + "\n");
			}

			logger.info(sb.toString());
		}

	}

	public static enum MetricsRecordType {
		SUCCESS, VALIDATION_FAILURE, EXECUTION_FAILURE, MESSAGE, WARNING;

		private MetricsRecordType() {
		}
	}

	public static class MetricsRecord {
		private final Date timestamp = new Date();
		private MetricsContext.MetricsRecordType recordType;
		private String sourceRuleInfo;
		private String contextInfo;
		private String message;
		private int rowNumber;
		private String columnName;

		public MetricsRecord() {
		}

		public String getMessage() {
			return this.message;
		}

		public void setMessage(String message) {
			this.message = message;
		}

		public MetricsContext.MetricsRecordType getRecordType() {
			return this.recordType;
		}

		public void setRecordType(MetricsContext.MetricsRecordType recordType) {
			this.recordType = recordType;
		}

		public String getSourceRuleInfo() {
			return this.sourceRuleInfo;
		}

		public void setSourceRuleInfo(String sourceRuleInfo) {
			this.sourceRuleInfo = sourceRuleInfo;
		}

		public String getContextInfo() {
			return this.contextInfo;
		}

		public void setContextInfo(String contextInfo) {
			this.contextInfo = contextInfo;
		}

		public Date getTimestamp() {
			return this.timestamp;
		}

		public int getRowNumber() {
			return this.rowNumber;
		}

		public void setRowNumber(int rowNumber) {
			this.rowNumber = rowNumber;
		}

		public String getColumnName() {
			return this.columnName;
		}

		public void setColumnName(String columnName) {
			this.columnName = columnName;
		}

		public String toString() {
			return (new ToStringBuilder(this)).append("message", this.message)
					.append("rowNumber", this.rowNumber)
					.append("columnName", this.columnName)
					.append("contextInfo", this.contextInfo)
					.append("sourceRuleInfo", this.sourceRuleInfo).toString();
		}
	}
}
