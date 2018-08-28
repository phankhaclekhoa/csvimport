/**
 * 
 */

/**
 * @author pklkhoa
 *
 */
package com.company;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonRootName;
import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonRootName("userfiledatareport")
public class UserFileDataReport {
	private Integer totalRows;
	private Integer importedRows;
	private Integer processingErrors;
	private List<String> errorMessages;

	public UserFileDataReport() {
		this.totalRows = Integer.valueOf(0);
		this.importedRows = Integer.valueOf(0);
		this.processingErrors = Integer.valueOf(0);
		this.errorMessages = new ArrayList();
	}

	public Integer getTotalRows() {
		return this.totalRows;
	}

	public void setTotalRows(Integer totalRows) {
		this.totalRows = totalRows;
	}

	public Integer getImportedRows() {
		return this.importedRows;
	}

	public void setImportedRows(Integer importedRows) {
		this.importedRows = importedRows;
	}

	public Integer getProcessingErrors() {
		return this.processingErrors;
	}

	public void setProcessingErrors(Integer processingErrors) {
		this.processingErrors = processingErrors;
	}

	public List<String> getErrorMessages() {
		return this.errorMessages;
	}

	public void setErrorMessages(List<String> errorMessages) {
		this.errorMessages = errorMessages;
	}

	public void addErrorMessage(String errorMessage) {
		this.errorMessages.add(errorMessage);
	}

	@ConstructorProperties({ "totalRows", "importedRows", "processingErrors",
			"errorMessages" })
	public UserFileDataReport(Integer totalRows, Integer importedRows,
			Integer processingErrors, List<String> errorMessages) {
		this.totalRows = totalRows;
		this.importedRows = importedRows;
		this.processingErrors = processingErrors;
		this.errorMessages = errorMessages;
	}
}