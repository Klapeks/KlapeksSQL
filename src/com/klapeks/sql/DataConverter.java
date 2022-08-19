package com.klapeks.sql;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import com.klapeks.sql.DataType.SimpleDataType;

@SuppressWarnings("rawtypes")
public class DataConverter {
	
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
		addConverter(byte.class, new SimpleDataType<>(Byte.class, "TINYINT"));
		addConverter(short.class, new SimpleDataType<>(Short.class, "SMALLINT"));
		addConverter(int.class, new SimpleDataType<>(Integer.class, "INT"));
		addConverter(long.class, new SimpleDataType<>(Long.class, "BIGINT"));
		addConverter(float.class, new SimpleDataType<>(Float.class, "FLOAT"));
		addConverter(double.class, new SimpleDataType<>(Double.class, "DOUBLE"));
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
		DataType dt = DataConverter.getConverter(o.getClass());
		if (o instanceof List<?>) {
			dt = DataConverter.getConverter(List.class);
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
		if (clazz==byte.class || clazz==Byte.class) {
			db_value = (byte) (int) db_value;
		}
		if (clazz==short.class || clazz==Short.class) {
			db_value = (short) (int) db_value;
		}
		DataType dt = DataConverter.getConverter(clazz);
		if (dt==null) return db_value;
		return dt.convertFromDB(db_value);
	}
}
