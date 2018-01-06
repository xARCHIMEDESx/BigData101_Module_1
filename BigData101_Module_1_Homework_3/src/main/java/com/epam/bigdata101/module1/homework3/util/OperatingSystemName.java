package com.epam.bigdata101.module1.homework3.util;

public enum OperatingSystemName {
	WINDOWS_XP(0), OTHER(1);
	
	private int code;	
	private OperatingSystemName(int code) {this.code= code;}	
	public int getCode() {return this.code;}
}
