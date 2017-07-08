package com.folkol;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Super simple "database" for when you really do not care that much.
 * <p>
 * (A Persistent Map that serializes objects with a bundled Gson.)
 * <p>
 */
public class SimpleDB<T> implements Map<String, T> {
    private static final String METADATA_PATH = "metadata.json";
    private static final String TYPE = "type";

    private Path basePath;
    private Class cls;
    private Path data;
    private Path metadataPath;

    private Gson gson = new Gson();

    /**
     * Initializes or creates the "database" in the given directory.
     *
     * @param db  the database directory
     * @param cls The class to be passed to Gson
     */
    public SimpleDB(String db, Class<? extends T> cls) {
        this.cls = cls;

        basePath = Paths.get(db);
        data = basePath.resolve("data");
        metadataPath = basePath.resolve(METADATA_PATH);

        if (Files.exists(basePath)) {
            reuseDataDirectory(cls);
        }

        try {
            createDataDirectory();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize data directory", e);
        }
    }

    private void createDataDirectory() throws IOException {
        Files.createDirectories(data);

        Map<String, String> metadata = new HashMap<>();
        metadata.put(TYPE, cls.getName());
        String json = gson.toJson(metadata);
        try (Writer writer = Files.newBufferedWriter(metadataPath, StandardCharsets.UTF_8)) {
            writer.write(json);
        }
    }

    private void reuseDataDirectory(Class<? extends T> cls) {
        if (!Files.isDirectory(basePath)) {
            throw new RuntimeException("Specified path is not a valid SimpleDB directory: " + basePath);
        }
        if (!Files.isDirectory(data)) {
            throw new RuntimeException("Specified path is not a valid data directory: " + data);
        }
        if (!Files.exists(metadataPath)) {
            throw new RuntimeException("Corrupt data directory, missing metadata file: " + metadataPath);
        }
        try {
            try (Reader reader = Files.newBufferedReader(metadataPath)) {
                Type type = new TypeToken<Map<String, String>>() {
                }.getType();
                Map<String, String> metadata = gson.fromJson(reader, type);
                String storedType = metadata.get(TYPE);
                String runtimeType = cls.getName();
                if (!runtimeType.equals(storedType)) {
                    throw new RuntimeException(String.format(
                        "Stored type (%s) is not the same as runtime type (%s).",
                        storedType,
                        runtimeType));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read database metadata", e);
        }
    }

    @Override
    public int size() {
        try (Stream files = Files.list(data)) {
            return (int) files.count();
        } catch (IOException e) {
            throw new RuntimeException("Failed to count the number of stored objects.", e);
        }
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return Files.exists(data.resolve((String) key));
    }

    @Override
    public boolean containsValue(Object value) {
        return containsKey(value);
    }

    @Override
    public T get(Object key) {
        Path path = data.resolve((String) key);
        if (!Files.exists(path)) {
            return null;
        }

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            return gson.fromJson(reader, (Class<T>) cls);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read value", e);
        }
    }

    @Override
    public T put(String key, T value) {
        Object oldValue = get(key);
        Path path = data.resolve(key);
        try {
            String json = gson.toJson(value);
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            Files.copy(bais, path, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist value", e);
        }
        return (T) oldValue;
    }

    @Override
    public T remove(Object key) {
        throw new RuntimeException("Operation not yet implemented");
    }

    @Override
    public void putAll(Map<? extends String, ? extends T> m) {
        for (Entry<? extends String, ? extends T> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public void clear() {
        try {
            Files.walk(data).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to remove file", e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException("Failed to clear the database", e);
        }
    }

    @Override
    public Set<String> keySet() {
        try (Stream<Path> list = Files.list(data)) {
            return list.map(data::relativize)
                       .map(Path::toString)
                       .collect(Collectors.toSet());
        } catch (IOException e) {
            throw new RuntimeException("Failed to list files", e);
        }
    }

    @Override
    public Collection<T> values() {
        try {
            try (Stream<Path> list = Files.list(data)) {
                return list.map(data::relativize)
                           .map(Path::toString)
                           .map(this::get)
                           .collect(Collectors.toList());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to produce a list of values", e);
        }
    }

    @Override
    public Set<Entry<String, T>> entrySet() {
        try (Stream<Path> list = Files.list(data)) {
            return list.map(data::relativize)
                       .map(Path::toString)
                       .map(key -> new AbstractMap.SimpleEntry<>(key, get(key)))
                       .collect(Collectors.toSet());
        } catch (IOException e) {
            throw new RuntimeException("Failed to retrieve entryset", e);
        }
    }
}
