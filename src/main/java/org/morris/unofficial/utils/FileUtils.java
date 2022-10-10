package org.morris.unofficial.utils;

import java.io.InputStream;
import java.net.URL;
import java.util.Objects;

/**
 * Utility class that provides easy access to Resources that contains Mock Notification Api Events in the form
 * of {@link URL} and {@link InputStream}.
 */
public class FileUtils {

    public static URL getResource(String file) {
        return FileUtils.class.getResource(file);
    }

    public static InputStream getResourceAsStream(String file) {
        return FileUtils.class.getResourceAsStream(file);
    }

    public static String getResourcePrefix(String file) {
        return Objects.requireNonNull(FileUtils.class.getResource(file)).toString();
    }
}
