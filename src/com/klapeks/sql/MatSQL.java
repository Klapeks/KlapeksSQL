package com.klapeks.sql;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.klapeks.sql.anno.Column;
import com.klapeks.sql.anno.Limit;
import com.klapeks.sql.anno.Nullable;
import com.klapeks.sql.anno.Primary;
import com.klapeks.sql.anno.Table;
import com.klapeks.sql.anno.Unique;

public class MatSQL extends Database {

	private Connection connection;
	@Override
	public void connect(String url, Properties properties) {
		disconnect();
		try {
			connection = DriverManager.getConnection(url, properties);
		} catch (SQLException e) {
			throw new RuntimeSQLException(e);
		}
	}

	@Override
	public void disconnect() {
		if (connection == null) return;
		try {
			connection.close();
		} catch (SQLException e) {
			throw new RuntimeSQLException(e);
		}
		connection = null;
	}

	@Override
	public boolean checkIfTableExists(Class<?> table) {
		try {
			StringBuilder query = new StringBuilder();
			query.append("SELECT count(*) FROM information_schema.tables");
			if (table != null) {
				Table t = table.getAnnotation(Table.class);
				query.append(" WHERE table_name = '");
				query.append(t.value());
				query.append("' AND table_schema = '");
				query.append(connection.getCatalog());
				query.append("' LIMIT 1;");
			}
            ResultSet rs = this.connection.prepareStatement(query.toString()).executeQuery();
            if (!rs.next()) return false;
            return rs.getInt(1) > 0;
        } catch (SQLException e) {
            throw new RuntimeSQLException(e);
        }
	}
	
	int getLimit(Field field, DataType<?> dt) {
		try {
			return field.getAnnotation(Limit.class).value();
		} catch (Throwable t) {
			return dt.defaultLimit();
		}
	}

	@Override
	public void createTable(Class<?> table) {
		StringBuilder query = new StringBuilder();
		query.append("CREATE TABLE `");
		query.append(table.getAnnotation(Table.class).value());
		query.append("` ( ");
		int index = 0;
		for (Field field : table.getDeclaredFields()) {
			Column column = field.getAnnotation(Column.class);
			if (column==null) continue;
			if (index++>0) query.append(" , ");
			query.append("`");
			query.append(column.value());
			query.append("` ");
			DataType<?> datatype = getConverter(field.getType());
			if (datatype==null) {
				if (field.getType().isAssignableFrom(List.class)) {
					datatype = getConverter(List.class);
				}
				throw new RuntimeException("Can't get converter for " + field.getType());
			}
			int limit = getLimit(field, datatype);
			query.append(datatype.getSqlType(limit));
			if (limit > 0) {
				query.append("(");
				query.append(limit);
				query.append(")");
			}
			query.append(" ");
			if (field.getAnnotation(Nullable.class) != null) query.append("NULL");
			else query.append("NOT NULL");
		}
		query.append(" );");
		try {
			this.connection.prepareStatement(query.toString()).executeUpdate();
		} catch (SQLException e) {
            throw new RuntimeSQLException(e);
		}
		updateTable(table);
	}

	@Override
	public void insert(Object object) {
		Table table = validTable(object);
		
		StringBuilder query = new StringBuilder();
		query.append("INSERT INTO `");
		query.append(table.value());
		query.append("` ( ");
		
		int index = 0;
		for (Field field : object.getClass().getDeclaredFields()) {
			Column column = field.getAnnotation(Column.class);
			if (column==null) continue;
			if (index++>0) query.append(" , ");
			query.append("`");
			query.append(column.value());
			query.append("`");
		}
		query.append(" ) VALUES ( ");
		for (int i = 0; i < index; i++) {
			if (i>0) query.append(" , ");
			query.append("?");
		}
		query.append(" );");
		try {
			PreparedStatement st = connection.prepareStatement(query.toString());
			index = 0;
			for (Field field : object.getClass().getDeclaredFields()) {
				Column column = field.getAnnotation(Column.class);
				if (column==null) continue;
				index++;
				try {
					field.setAccessible(true);
					st.setObject(index, convertToDB(field.get(object)));
				} catch (Throwable e) {
					e.printStackTrace();
				}
			}
			st.executeUpdate();
		} catch (SQLException e) {
            throw new RuntimeSQLException(e);
		}
	
	}

	@Override
	public void update(Object object, Where where) {
		Table table = validTable(object);
		
		StringBuilder query = new StringBuilder();
		query.append("UPDATE `");
		query.append(table.value());
		query.append("` SET ");
		List<Object> placeholders = new ArrayList<>();
		int index = 0;
		for (Field field : object.getClass().getDeclaredFields()) {
			Column column = field.getAnnotation(Column.class);
			if (column==null) continue;
			if (index++>0) query.append(" , ");
			try {
				field.setAccessible(true);
				Object a = field.get(object);
				query.append("`");
				query.append(column.value());
				query.append("` = ?");
				placeholders.add(convertToDB(a));
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
		query.append(" WHERE ");
		query.append(where.query);
		for (Object o : where.placeholders) {
			placeholders.add(o);
		}
		try {
			PreparedStatement st = connection.prepareStatement(query.toString());
			index = 0;
			for (Object o : placeholders) {
				st.setObject(++index, convertToDB(o));
			}
			st.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeSQLException(e);
		}
	
	}

	@Override
	public <T> List<T> select(Class<T> table, Where where) {
		StringBuilder query = new StringBuilder();
		query.append("SELECT * FROM `");
		query.append(validTable(table).value());
		query.append("` WHERE ");
		query.append(where.query);
		if (where.limit > 0) {
			query.append(" LIMIT ");
			query.append(where.limit);
		}
		try {
			PreparedStatement st = connection.prepareStatement(query.toString());
			int index = 0;
			for (Object o : where.placeholders) {
				st.setObject(++index, convertToDB(o));
			}
			ResultSet result = st.executeQuery();
			List<T> list = new ArrayList<>();
			while (result.next()) {
				list.add(generateFromResultSet(table, result));
				while(list.contains(null)) list.remove(null);
			}
			return list;
		} catch (SQLException e) {
			throw new RuntimeSQLException(e);
		}
	}
	
	@Override
	public boolean hasOne(Class<?> table, Where where) {
		StringBuilder query = new StringBuilder();
		query.append("SELECT * FROM `");
		query.append(validTable(table).value());
		query.append("` WHERE ");
		query.append(where.query);
		if (where.limit > 0) {
			query.append(" LIMIT ");
			query.append(where.limit);
		}
		try {
			PreparedStatement st = connection.prepareStatement(query.toString());
			int index = 0;
			for (Object o : where.placeholders) {
				st.setObject(++index, convertToDB(o));
			}
			ResultSet result = st.executeQuery();
			return result.next();
		} catch (SQLException e) {
			throw new RuntimeSQLException(e);
		}
	}
	


	static <T> T generateFromResultSet(Class<T> clazz, ResultSet result) {
		try {
			Constructor<T> constr = clazz.getConstructor();
			constr.setAccessible(true);
			T object = constr.newInstance();
			for (Field field : clazz.getDeclaredFields()) {
				Column column = field.getAnnotation(Column.class);
				if (column==null) continue;
				field.setAccessible(true);
				Object value = result.getObject(column.value());
				value = convertFromDB(field.getType(), value);
				field.set(object, value);
			}
			return object;
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}
	
	private void updateTable(Class<?> table) {
		StringBuilder query = new StringBuilder();
		query.append("ALTER TABLE `");
		query.append(validTable(table).value());
		query.append("` ");
		
		int index = 0;
		StringBuilder primaryKeys = null;
		for (Field field : table.getDeclaredFields()) {
			Column column = field.getAnnotation(Column.class);
			if (column==null) continue;

			if (field.getAnnotation(Primary.class)!=null) {
				if (primaryKeys!=null) primaryKeys.append(", ");
				else primaryKeys = new StringBuilder();
				primaryKeys.append("`");
				primaryKeys.append(column.value());
				primaryKeys.append("`");
			}
			Unique unique = field.getAnnotation(Unique.class);
			if (unique!=null) {
				if (index++ > 0) query.append(", ");
				String val = unique.value();
				if (val==null||val.isEmpty()) val = column.value();
				query.append("ADD UNIQUE KEY `");
				query.append(unique.value().isEmpty() ? column.value() : unique.value());
				query.append("` (`");
				query.append(column.value());
				query.append("`)");
			}
		}
		if (primaryKeys == null && index <= 0) return;
		if (primaryKeys!=null) {
			if (index++ > 0) query.append(", ");
			query.append("ADD PRIMARY KEY (");
			query.append(primaryKeys);
			query.append(")");
		}
		System.out.println(query);
		try {
			this.connection.prepareStatement(query.toString()).executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
//	public void alterTalbe(Class<?> table, String query) {
//		try {
//			StringBuilder sb = new StringBuilder();
//			sb.append("ALTER TABLE `");
//			sb.append(validTable(table).value());
//			sb.append("` ");
//			sb.append(query);
//			this.connection.prepareStatement(sb.toString()).executeUpdate();
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
//	private boolean updatePrimaryConstraint(Class<?> table) {
//		Table t = validTable(table);
//		PrimaryConstraint pc = table.getAnnotation(PrimaryConstraint.class);
//		if (pc==null) return false;
//
//		StringBuilder query = new StringBuilder();
//		query.append("ALTER TABLE `");
//		query.append(t.value());
//		query.append("` ADD CONSTRAINT ");
//		query.append(pc.value());
//		query.append(" PRIMARY KEY ( ");
//		int index = 0;
//		for (Field field : table.getDeclaredFields()) {
//			Column column = field.getAnnotation(Column.class);
//			if (column==null) continue;
//			if (field.getAnnotation(Primary.class)==null) continue;
//			if (index++ > 0) query.append(" , ");
//			query.append("`");
//			query.append(column.value());
//			query.append("`");
//		}
//		query.append(" );");
//		try {
//			this.connection.prepareStatement(query.toString()).executeUpdate();
//			return true;
//		} catch (SQLException e) {
//            throw new RuntimeSQLException(e);
//		}
//	
//	}
}
