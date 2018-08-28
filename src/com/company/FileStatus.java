package com.company;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public enum FileStatus {
    UPLOAD_FAILED(Collections.EMPTY_LIST),
    INVALID(Collections.EMPTY_LIST),
    PROCESSING_FAILED(Collections.EMPTY_LIST),
    PROCESSING_SUCCESSFUL(Collections.EMPTY_LIST),
    PROCESSING(Arrays.asList(new FileStatus[]{PROCESSING_FAILED, PROCESSING_SUCCESSFUL})),
    UPLOADED(Arrays.asList(new FileStatus[]{INVALID, PROCESSING, PROCESSING_FAILED})),
    UPLOADING(Arrays.asList(new FileStatus[]{UPLOADED, UPLOAD_FAILED, PROCESSING_FAILED})),
    CREATED(Arrays.asList(new FileStatus[]{UPLOADED, UPLOAD_FAILED, UPLOADING}));

    private List<FileStatus> nextStatus;

    private FileStatus(List<FileStatus> nextStatus) {
        this.nextStatus = nextStatus;
    }

    public static FileStatus fromString(String value) {
        if(!StringUtils.isEmpty(value)) {
            FileStatus[] var1 = values();
            int var2 = var1.length;

            for(int var3 = 0; var3 < var2; ++var3) {
                FileStatus fileStatus = var1[var3];
                if(value.equalsIgnoreCase(fileStatus.toString())) {
                    return fileStatus;
                }
            }
        }

        return null;
    }

    public List<FileStatus> getNextStatus() {
        return this.nextStatus;
    }
}