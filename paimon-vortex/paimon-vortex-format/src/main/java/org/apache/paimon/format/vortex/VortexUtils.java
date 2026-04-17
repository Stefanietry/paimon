/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.format.vortex;

import org.apache.paimon.fs.*;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Pair;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

/** Utils for Vortex storage options. */
public class VortexUtils {

    private static final Class<?> ossFileIOKlass;
    private static final Class<?> pluginFileIO;
    private static final Class<?> jindoFileIOKlass;

    static {
        Class<?> klass;
        try {
            klass = Class.forName("org.apache.paimon.oss.OSSFileIO");
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            klass = null;
        }
        ossFileIOKlass = klass;

        try {
            klass = Class.forName("org.apache.paimon.jindo.JindoFileIO");
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            klass = null;
        }
        jindoFileIOKlass = klass;

        try {
            klass = Class.forName("org.apache.paimon.fs.PluginFileIO");
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            klass = null;
        }
        pluginFileIO = klass;
    }

    public static Pair<Path, Map<String, String>> toVortexSpecified(FileIO fileIO, Path path) {
        URI uri = path.toUri();
        String schema = uri.getScheme();

        if (fileIO instanceof RESTTokenFileIO) {
            try {
                fileIO = ((RESTTokenFileIO) fileIO).fileIO();
            } catch (IOException e) {
                throw new RuntimeException("Can't get fileIO from RESTTokenFileIO", e);
            }
        }

        Options originOptions;
        if (ossFileIOKlass != null && ossFileIOKlass.isInstance(fileIO)) {
            originOptions = invokeHadoopOptions(fileIO);
        } else if (jindoFileIOKlass != null && jindoFileIOKlass.isInstance(fileIO)) {
            originOptions = invokeHadoopOptions(fileIO);
        } else if (pluginFileIO != null && pluginFileIO.isInstance(fileIO)) {
            originOptions = ((PluginFileIO) fileIO).options();
        } else {
            originOptions = new Options();
        }

        Path converted = path;
        Map<String, String> storageOptions = new HashMap<>();
        if ("oss".equals(schema)) {
            storageOptions.put(
                    "endpoint",
                    "https://" + uri.getHost() + "." + originOptions.get("fs.oss.endpoint"));
            storageOptions.put("access_key_id", originOptions.get("fs.oss.accessKeyId"));
            storageOptions.put("secret_access_key", originOptions.get("fs.oss.accessKeySecret"));
            storageOptions.put("virtual_hosted_style_request", "true");
            if (originOptions.containsKey("fs.oss.securityToken")) {
                storageOptions.put("session_token", originOptions.get("fs.oss.securityToken"));
            }
            converted = new Path(uri.toString().replace("oss://", "s3://"));
        }

        return Pair.of(converted, storageOptions);
    }

    public static boolean isHdfsScheme(String scheme) {
        return "hdfs".equalsIgnoreCase(scheme) || "viewfs".equalsIgnoreCase(scheme);
    }

    /** Create a local temp file path for Vortex native writer to create. */
    public static java.nio.file.Path createLocalTempFileForWrite() throws IOException {
        java.nio.file.Path tmp = Files.createTempFile("paimon-vortex-", ".vortex");
        // Vortex writer expects to create the file itself.
        Files.deleteIfExists(tmp);
        return tmp;
    }

    /** Download a remote file (e.g. HDFS) to local tmp for Vortex native reader. */
    public static java.nio.file.Path downloadToLocalFile(FileIO fileIO, Path sourcePath)
            throws IOException {
        java.nio.file.Path tmp = Files.createTempFile("paimon-vortex-", ".vortex");
        try (SeekableInputStream in = fileIO.newInputStream(sourcePath);
                OutputStream out = Files.newOutputStream(tmp)) {
            IOUtils.copyBytes(in, out, IOUtils.BLOCKSIZE, false);
        } catch (IOException e) {
            deleteLocalFileQuietly(tmp);
            throw e;
        }
        return tmp;
    }

    /** Upload a local file created by Vortex native writer to remote filesystem (e.g. HDFS). */
    public static void uploadLocalFile(
            FileIO targetFileIO, java.nio.file.Path localFile, Path target) throws IOException {
        boolean success = false;
        try {
            Path parent = target.getParent();
            if (parent != null) {
                targetFileIO.mkdirs(parent);
            }

            try (InputStream in = Files.newInputStream(localFile);
                    PositionOutputStream out = targetFileIO.newOutputStream(target, false)) {
                IOUtils.copyBytes(in, out, IOUtils.BLOCKSIZE, false);
            }
            success = true;
        } finally {
            if (!success) {
                targetFileIO.deleteQuietly(target);
            }
        }
    }

    public static void deleteLocalFileQuietly(java.nio.file.Path localFile) {
        if (localFile == null) {
            return;
        }
        try {
            Files.deleteIfExists(localFile);
        } catch (Exception ignored) {
            // ignore
        }
    }

    public static Path toLocalPath(java.nio.file.Path localFile) {
        return new Path(localFile.toUri().toString());
    }

    private static Options invokeHadoopOptions(Object fileIO) {
        try {
            return (Options) fileIO.getClass().getMethod("hadoopOptions").invoke(fileIO);
        } catch (Exception e) {
            throw new RuntimeException("Failed to invoke hadoopOptions", e);
        }
    }
}
