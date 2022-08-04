package com.klapeks.sql;

import java.util.List;

public class Where implements Cloneable {

	String query;
	Object[] placeholders;
	int limit = -1;
	public Where(String query, Object... placeholders) {
		this.query = query;
		this.placeholders = placeholders;
	}
	public Where(String query, List<Object> placeholders) {
		this(query, toArray(placeholders));
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
	
	@Override
	public boolean equals(Object obj) {
		if (obj==null) return false;
		if (!(obj instanceof Where)) return false;
		Where where = (Where) obj;
		if (where == this) return true;
		if (where.placeholders.length!=this.placeholders.length) return false;
		if (!where.query.equals(query)) return false;
		for (int i = 0; i < where.placeholders.length; i++) {
			if (!where.placeholders[i].equals(placeholders[i])) return false;
		}
		return true;
	}
	@Override
	public int hashCode() {
		return query.hashCode();
	}
	
	private static Object[] toArray(List<Object> list) {
		Object[] objs = new Object[list.size()];
		for (int i = list.size() - 1; i >= 0; i--) {
			objs[i] = list.get(i);
		}
		return objs;
	}
}
