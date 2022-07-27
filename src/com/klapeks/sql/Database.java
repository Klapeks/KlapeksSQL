package com.klapeks.sql;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import com.klapeks.sql.anno.Column;
import com.klapeks.sql.anno.Primary;
import com.klapeks.sql.anno.Table;

public abstract class Database {
	
	public abstract void connect(String path, Properties properties);
	public void connect(String path, String username, String password) {
		Properties properties = new Properties();
		properties.setProperty("user", username);
		properties.setProperty("password", password);
		properties.setProperty("characterEncoding", "utf8");
		connect(path, properties);
	}
	public abstract void disconnect();

	public abstract boolean checkIfTableExists(Class<?> table);
	public abstract void createTable(Class<?> table);
	public void createTableIfNotExists(Class<?> table) {
		if (checkIfTableExists(table)) return;
		createTable(table);
	}

	public abstract void insert(Object object);
	public abstract void update(Object object, Where where);

	public void update(Object object) {
		validTable(object);
		update(object, generateWhere(object));
	}
	public void updateOrInsert(Object object, Where where) {
		validTable(object);
		if (selectOne(object.getClass(), where)==null) {
			insert(object);
			return;
		}
		update(object, where);
	}
	public void updateOrInsert(Object object) {
		validTable(object);
		updateOrInsert(object, generateWhere(object));
	}

	public abstract <T> List<T> select(Class<T> table, Where where);
	public <T> T selectOne(Class<T> table, Where where) {
		where.limit = 1;
		List<T> list = select(table, where);
		if (list == null || list.isEmpty()) return null;
		return list.get(0);
	}
	

	static Object convertTo(Object o) {
		if (o instanceof List<?>) {
			o = ((List<?>) o).stream().map(s -> s+"").collect(Collectors.joining("\n"));
		}
		return o;
	}
	
	static Table validTable(Object object) {
		Class<?> clazz = object.getClass();
		if (clazz == Class.class) clazz = (Class<?>) object;
		Table table = clazz.getAnnotation(Table.class);
		if (table==null) throw new RuntimeException(clazz + " is not Table");
		return table;
	}
	public static Where generateWhere(Object object) {
		List<Object> placeholders = new ArrayList<>();
		StringBuilder query = new StringBuilder();
		int index = 0;
		for (Field field : object.getClass().getDeclaredFields()) {
			Column column = field.getAnnotation(Column.class);
			if (column==null) continue;
			if (field.getAnnotation(Primary.class)==null) continue;
			if (index++>0) query.append(" AND ");
			try {
				field.setAccessible(true);
				placeholders.add(convertTo(field.get(object)));
				query.append("`");
				query.append(column.value());
				query.append("` = ?");
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
		return new Where(query.toString(), placeholders);
	}

	public Where where(String query, Object... placeholders) {
		return new Where(query, placeholders);
	}
}
