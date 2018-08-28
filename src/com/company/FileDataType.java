package com.company;
public enum FileDataType {
	CUSTOMER;
	
	private FileDataType() {
    }

    public static FileDataType fromString(String value) {
        if(!StringUtils.isEmpty(value)) {
            FileDataType[] var1 = values();
            int var2 = var1.length;

            for(int var3 = 0; var3 < var2; ++var3) {
                FileDataType fileDataType = var1[var3];
                if(value.equalsIgnoreCase(fileDataType.toString())) {
                    return fileDataType;
                }
            }
        }

        return null;
    }
}
