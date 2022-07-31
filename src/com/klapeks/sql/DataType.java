package com.klapeks.sql;

public interface DataType<T extends Object> {
	
	public String getSqlType(int limit);
	public Class<T> getTypeClass();

	public T convertFromDB(Object objectFromDataBase);
	public Object convertToDB(T objectThatWillBeSavedInDB);
	
	public default int defaultLimit() {
		return -1;
	}
	
	public static class SimpleDataType<T extends Object> implements DataType<T> {
		String type;
		Class<T> clazz;
		public SimpleDataType(Class<T> clazz, String type) {
			this.clazz = clazz;
			this.type = type;
		}

		@Override
		public String getSqlType(int limit) {
			return type;
		}

		@Override
		public Class<T> getTypeClass() {
			return clazz;
		}

		@SuppressWarnings("unchecked")
		@Override
		public T convertFromDB(Object objectFromDataBase) {
			return (T) objectFromDataBase;
		}

		@Override
		public Object convertToDB(T objectThatWillBeSavedInDB) {
			return objectThatWillBeSavedInDB;
		}
	}
}
