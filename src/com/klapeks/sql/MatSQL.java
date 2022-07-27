package com.klapeks.sql;

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
import com.klapeks.sql.anno.Primary;
import com.klapeks.sql.anno.PrimaryConstraint;
import com.klapeks.sql.anno.Table;

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
			Table t = table.getAnnotation(Table.class);
            String query = "SELECT count(*) FROM information_schema.tables WHERE table_name = '" 
            		+ t.value() + "' AND table_schema = '" + connection.getCatalog() + "' LIMIT 1;";
            ResultSet rs = this.connection.prepareStatement(query).executeQuery();
            int records = 0;
            if (rs.next()) records = rs.getInt(1);
            return records > 0;
        } catch (SQLException e) {
            throw new RuntimeSQLException(e);
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
			if (field.getType() == Integer.class) query.append("INT");
			if (field.getType() == Long.class) query.append("BIGINT");
			if (field.getType() == int.class) query.append("INT");
			if (field.getType() == long.class) query.append("BIGINT");
			if (field.getType() == String.class) {
				Limit limit = field.getAnnotation(Limit.class);
				if (limit!=null) {
					query.append("VARCHAR(");
					query.append(limit.value());
					query.append(")");
				} else query.append("TEXT");
			}
			else if (field.getType().isAssignableFrom(List.class)) {
				query.append("TEXT");
			}
			query.append(" NOT NULL");
		}
		query.append(" );");
		try {
			this.connection.prepareStatement(query.toString()).executeUpdate();
		} catch (SQLException e) {
            throw new RuntimeSQLException(e);
		}
		updateTable(table);
	}
	private void updateTable(Class<?> table) {
		PrimaryConstraint pc = table.getAnnotation(PrimaryConstraint.class);
		Table t = table.getAnnotation(Table.class);
		if (pc!=null) {
			StringBuilder query = new StringBuilder();
			query.append("ALTER TABLE `");
			query.append(t.value());
			query.append("` ADD CONSTRAINT ");
			query.append(pc.value());
			query.append(" PRIMARY KEY ( ");
			int index = 0;
			for (Field field : table.getDeclaredFields()) {
				Column column = field.getAnnotation(Column.class);
				if (column==null) continue;
				if (field.getAnnotation(Primary.class)==null) continue;
				if (index++ > 0) query.append(" , ");
				query.append("`");
				query.append(column.value());
				query.append("`");
			}
			query.append(" );");
			try {
				this.connection.prepareStatement(query.toString()).executeUpdate();
			} catch (SQLException e) {
	            throw new RuntimeSQLException(e);
			}
		}
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
					st.setObject(index, convertTo(field.get(object)));
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
				placeholders.add(convertTo(a));
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
				st.setObject(++index, o);
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
				st.setObject(++index, o);
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
	


	@SuppressWarnings("unchecked")
	static <T> T generateFromResultSet(Class<T> clazz, ResultSet result) {
		try {
			T t = clazz.getConstructor().newInstance();
			for (Field field : clazz.getDeclaredFields()) {
				Column column = field.getAnnotation(Column.class);
				if (column==null) continue;
				field.setAccessible(true);
				Object o = result.getObject(column.value());
				if (field.getType().isAssignableFrom(List.class)) {
					String[] g = o.toString().replace("\r", "").split("\n");
					o = new ArrayList<Object>();
					for (String s : g) {
						((List<Object>) o).add(s);
					}
				}
				field.set(t, o);
			}
			return t;
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}
}
