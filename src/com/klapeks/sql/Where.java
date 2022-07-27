package com.klapeks.sql;

import java.util.List;

import com.klapeks.db.Cfg;

public class Where implements Cloneable {

	String query;
	Object[] placeholders;
	int limit = -1;
	public Where(String query, Object... placeholders) {
		this.query = query;
		this.placeholders = placeholders;
	}
	public Where(String query, List<Object> placeholders) {
		this(query, Cfg.toArray(placeholders));
	}
	
	public String getQuery() {
		return query;
	}
	
	public Object[] getPlaceholders() {
		return placeholders;
	}
	
	public int getLimit() {
		return limit;
	}
	public void setLimit(int limit) {
		this.limit = limit;
	}
	
	@Override
	public Where clone() {
		Where wh = new Where(this.query, this.placeholders.clone());
		wh.limit = this.limit;
		return wh;
	}
}
