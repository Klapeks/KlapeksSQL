package com.klapeks.sql;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.klapeks.sql.anno.Column;
import com.klapeks.sql.anno.Limit;
import com.klapeks.sql.anno.Nullable;
import com.klapeks.sql.anno.Primary;
import com.klapeks.sql.anno.Unique;

import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString
@EqualsAndHashCode
public class ColumnSchema {
	
	String name;
	String type;
	
	boolean isNullable;
	boolean isPrimary;
	boolean isUnique;
	
	public ColumnSchema() {}
	public ColumnSchema(Field field) {
		name = field.getAnnotation(Column.class).value();
		isPrimary = field.getAnnotation(Primary.class)!=null;
		isUnique = field.getAnnotation(Unique.class)!=null;
		isNullable = field.getAnnotation(Nullable.class)!=null;
		
		DataType<?> datatype = DataConverter.getConverter(field.getType());
		if (datatype==null) {
			if (field.getType().isAssignableFrom(List.class)) {
				datatype = DataConverter.getConverter(List.class);
			}
			throw new RuntimeException("Can't get converter for " + field.getType());
		}
		int limit = getLimit(field, datatype);
		type = datatype.getSqlType(limit);
		if (limit > 0) type += "(" + limit + ")";
	}
	public ColumnSchema(ResultSet set) {
		try {
			name = set.getObject("COLUMN_NAME").toString();
			type = set.getObject("COLUMN_TYPE").toString().toUpperCase();
			isNullable = "YES".equals(set.getObject("IS_NULLABLE"));
			isPrimary = "PRI".equals(set.getObject("COLUMN_KEY"));
			isUnique = "UNI".equals(set.getObject("COLUMN_KEY"));
		} catch (SQLException e) {
			throw new RuntimeSQLException(e);
		}
	}
	
	static int getLimit(Field field, DataType<?> dt) {
		try {
			return field.getAnnotation(Limit.class).value();
		} catch (Throwable t) {
			return dt.defaultLimit();
		}
	}
}
