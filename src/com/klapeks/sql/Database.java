package com.klapeks.sql;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import com.klapeks.sql.DataType.SimpleDataType;
import com.klapeks.sql.anno.Column;
import com.klapeks.sql.anno.Primary;
import com.klapeks.sql.anno.Table;

@SuppressWarnings("rawtypes")
public abstract class Database {
	
	static Map<Class<?>, DataType<?>> class$type = new HashMap<>();

	public static void addConverter(DataType<?> converter) {
		class$type.put(converter.getTypeClass(), converter);
	}
	public static void addConverter(Class<?> primClass, DataType<?> converter) {
		addConverter(converter);
		class$type.put(primClass, converter);
	}
	public static DataType<?> getConverter(Class<?> clazz) {
		if (clazz.isEnum()) return class$type.get(Enum.class);
		return class$type.get(clazz);
	}
	static {
		addConverter(int.class, new SimpleDataType<>(Integer.class, "INT"));
		addConverter(long.class, new SimpleDataType<>(Long.class, "BIGINT"));
		addConverter(new SimpleDataType<>(Timestamp.class, "TIMESTAMP"));
		addConverter(new SimpleDataType<String>(String.class, "TEXT") {
			@Override
			public String getSqlType(int limit) {
				return limit <= 0 ? "TEXT" : "VARCHAR";
			}
		});
		addConverter(new DataType<UUID>() {
			@Override
			public String getSqlType(int limit) {
				return "VARCHAR";
			}
			@Override
			public int defaultLimit() {
				return 36;
			}
			@Override
			public Class<UUID> getTypeClass() {
				return UUID.class;
			}
			@Override
			public UUID convertFromDB(Object obj) {
				return UUID.fromString(obj.toString());
			}
			@Override
			public Object convertToDB(UUID uuid) {
				return uuid.toString();
			}
		});
		addConverter(new DataType<Date>() {
			@Override
			public String getSqlType(int limit) {
				return "TIMESTAMP";
			}
			@Override
			public Class<Date> getTypeClass() {
				return Date.class;
			}
			@Override
			public Date convertFromDB(Object obj) {
				return Timestamp.valueOf(obj.toString());
			}
			@Override
			public Object convertToDB(Date date) {
				if (date instanceof Timestamp) return date;
				return new Timestamp(date.getTime());
			}
		});
		addConverter(new DataType<List>() {
			@Override
			public String getSqlType(int limit) {
				return "TIMESTAMP";
			}
			@Override
			public Class<List> getTypeClass() {
				return List.class;
			}
			@Override
			public List<?> convertFromDB(Object obj) {
				if (obj instanceof List<?>) {
					return (List<?>) obj;
				}
				List<String> list = new ArrayList<>();
				String[] g = obj.toString().replace("\r", "").split("\n");
				for (String s : g) {
					if (s.isEmpty()) continue;
					list.add(s);
				}
				return list;
			}
			@SuppressWarnings("unchecked")
			@Override
			public Object convertToDB(List list) {
				return list.stream().map(s -> s+"").collect(Collectors.joining("\n"));
			}
		});
		addConverter(new DataType<Enum>() {
			@Override
			public String getSqlType(int limit) {
				return "VARCHAR";
			}
			public int defaultLimit() {
				return 32;
			}
			@Override
			public Class<Enum> getTypeClass() {
				return Enum.class;
			}
			@Override
			public Enum convertFromDB(Object obj) {
				throw new RuntimeException("Can't convert enum from object. Please use something another");
			}
			@Override
			public Object convertToDB(Enum e) {
				return e.name();
			}
		});
		addConverter(ObjectID.class, new DataType<ObjectID>() {
			@Override
			public String getSqlType(int limit) {
				return "VARCHAR";
			}
			public int defaultLimit() {
				return 16;
			};
			@Override
			public Class<ObjectID> getTypeClass() {
				return ObjectID.class;
			}
			@Override
			public ObjectID convertFromDB(Object objectFromDataBase) {
				return new ObjectID(objectFromDataBase.toString());
			}
			@Override
			public Object convertToDB(ObjectID objectThatWillBeSavedInDB) {
				return objectThatWillBeSavedInDB.hexID();
			}
		});
	}

	@SuppressWarnings("unchecked")
	static Object convertToDB(Object o) {
		if (o==null) return null;
		if (o.getClass().isEnum()) {
			return ((Enum<?>) o).name();
		}
		DataType dt = getConverter(o.getClass());
		if (o instanceof List<?>) {
			dt = getConverter(List.class);
		}
		if (dt == null) return o;
		return dt.convertToDB(o);
	}
	/**
	 * 
	 * @param clazz - Type class
	 * @param db_value - often string
	 * @return
	 */
	static <T> Object convertFromDB(Class<T> clazz, Object db_value) {
		if (db_value==null) return null;
		if (clazz.isEnum()) {
			Object[] enums = clazz.getEnumConstants();
			db_value = db_value.toString();
			for (int i = 0; i < enums.length; i++) {
				if (((Enum<?>) enums[i]).name().equalsIgnoreCase(db_value.toString())) {
					db_value = enums[i];
					break;
				}
			}
			if (!db_value.getClass().isEnum()) return null;
			return db_value;
		}
		DataType dt = getConverter(clazz);
		if (dt==null) return db_value;
		return dt.convertFromDB(db_value);
	}
	
//	public static <T> T convertFrom(Object obj)
	
	public abstract void connect(String path, Properties properties);
	public void connect(String path, String username, String password) {
		Properties properties = new Properties();
		properties.setProperty("user", username);
		properties.setProperty("password", password);
		properties.setProperty("characterEncoding", "utf8");
		properties.setProperty("autoReconnect", "true");
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
		if (!hasOne(object.getClass(), where)) {
			insert(object);
			return;
		}
		update(object, where);
	}
	public void updateOrInsert(Object object) {
		if (object==null) return;
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
	public abstract boolean hasOne(Class<?> table, Where where);
	
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
				placeholders.add(field.get(object));
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
