package com.klapeks.sql;

public class ObjectID {
	
	long id;
	public ObjectID() {
		this.id = System.currentTimeMillis();
	}
	public ObjectID(String id) {
		this.id = Long.parseLong(id, 16);
	}
	
	public String hexID() {
		return Long.toString(id, 16);
	}
	
}
