package com.freitas.hadoop.counter;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum ExceptionTypeEnum {
	
	PLACEHOLDER(0, "PLACEHOLDER", "", ""),
	UNKNOWN_ERROR(1, "UNKNOWN_ERROR", "An unknown error was encountered", "error.unknown"),
	INVALID_PARAMS(2, "INVALID_PARAMS", "An invalid parameter was received", "error.params.invalid"),
	NOT_FOUND(3, "NOT_FOUND", "Item of interest was not found", "error.notfound"),
	NULL_POINTER(4, "NULL_POINTER", "Null pointer encountered", "error.npe"),
	DUP_DATE(5, "DUP_DATE", "Duplicate date encountered", "error.dup.date"),
	INVALID_DATE(6, "INVALID_DATE", "Invalid date encountered", "error.date.invalid")
	;
	
	// lookup table to be used to find enum for conversion
	private static final Map<Long,ExceptionTypeEnum> lookup = new HashMap<Long,ExceptionTypeEnum>();
	static {
		for(ExceptionTypeEnum e : EnumSet.allOf(ExceptionTypeEnum.class))
			lookup.put(e.getErrorCode(), e);
	}
	
	
	private long errorCode;
	private String name;
	private String message;
	private String i18nKey;
	
	ExceptionTypeEnum(long errorCode, String name, String message, String i18nKey) {
		this.errorCode = errorCode;
		this.name = name;
		this.message = message;
		this.i18nKey = i18nKey;
	}
	
	public long getErrorCode() {
		return this.errorCode;
	}
	
	public String getName() {
		return this.name;
	}
	
	public String getMessage() {
		return this.message;
	}
	
	public String getI18nKey() {
		return this.i18nKey;
	}
	
	public static ExceptionTypeEnum get(long errorCode) { 
		return lookup.get(errorCode); 
	}

}
