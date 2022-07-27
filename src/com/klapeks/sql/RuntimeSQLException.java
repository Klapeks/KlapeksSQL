package com.klapeks.sql;

import java.sql.SQLException;

@SuppressWarnings("serial")
public class RuntimeSQLException extends RuntimeException {
	
	public RuntimeSQLException(SQLException e) {
		super(e);
	}

}
