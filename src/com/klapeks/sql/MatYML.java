package com.klapeks.sql;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.configuration.file.YamlConfiguration;

import com.klapeks.db.Cfg;
import com.klapeks.libs.Main;
import com.klapeks.sql.anno.Column;
import com.klapeks.sql.anno.IfYaml;
import com.klapeks.sql.anno.Table;

public class MatYML extends Database {
	
	Map<Class<?>, FileConfiguration> tables = new HashMap<>();

	@Override
	public void connect(String path, Properties properties) {}

	@Override
	public void disconnect() {
		tables.forEach((table, cfg) -> {
			try {
				cfg.save(getTablePath(table));
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		tables.clear();
	}
	

	@Override
	public boolean checkIfTableExists(Class<?> table) {
		return tables.containsKey(table);
	}

	@Override
	public void createTable(Class<?> table) {
		File file = getTablePath(table);
		if (!file.exists()) {
			file.getParentFile().mkdirs();
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		FileConfiguration cfg = YamlConfiguration.loadConfiguration(file);
		tables.put(table, cfg);
	}

	@Override
	public void insert(Object object) {
		update(object, generateWhere(object));
	}

	@Override
	public void update(Object object, Where where) {
		Table table = validTable(object);
		FileConfiguration cfg = tables.get(object.getClass());
		if (cfg == null) throw new RuntimeException("Unknown table: " + table.value());
		String key = parseWhere(where);
		for (Field field : object.getClass().getDeclaredFields()) {
			Column column = field.getAnnotation(Column.class);
			if (column==null) continue;
			field.setAccessible(true);
			try {
				Object a = field.get(object);
				cfg.set(key+"."+column.value(), a);
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
		try {
			cfg.save(getTablePath(object.getClass()));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public <T> List<T> select(Class<T> table, Where where) {
		Table t = validTable(table);
		FileConfiguration cfg = tables.get(table);
		if (cfg == null) throw new RuntimeException("Unknown table: " + t.value());
		List<T> list = new ArrayList<>();
		parse(cfg.getConfigurationSection(parseWhere(where)), table, list);
		if (list == null || list.isEmpty()) return null;
		return list;
	}
	private <T> void parse(ConfigurationSection section, Class<T> clazz, List<T> addTo) {
		boolean b = false;
		for (String key : section.getKeys(false)) {
			if (!section.isConfigurationSection(key)) continue;
			b = true;
			parse(section.getConfigurationSection(key), clazz, addTo);
		}
		try {
			if (!b) addTo.add(generateFromSection(clazz, section));
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (Throwable t) {}
	}

	static <T> T generateFromSection(Class<T> clazz, ConfigurationSection section) {
		try {
			T t = clazz.getConstructor().newInstance();
			for (Field field : clazz.getDeclaredFields()) {
				Column column = field.getAnnotation(Column.class);
				if (column==null) continue;
				field.setAccessible(true);
				field.set(t, section.get(column.value()));
			}
			return t;
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}
	static File getTablePath(Class<?> table) {
		Table t = validTable(table);
		IfYaml yaml = table.getAnnotation(IfYaml.class);
		if (yaml==null) return new File(Main.plugin.getDataFolder(), "db/"+t.value()+".yml");
		String path = yaml.value();
		if (!path.endsWith(".yml")) path+=".yml";
		if (path.startsWith("~")) return new File(path.substring(1));
		return new File(Main.plugin.getDataFolder(), "db/"+path);
	}
	
	static String parseWhere(Where where) {
		return Cfg.join(".", where.placeholders);
	}
}
