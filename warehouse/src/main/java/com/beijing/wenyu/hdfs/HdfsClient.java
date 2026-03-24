package com.beijing.wenyu.hdfs;

import com.beijing.wenyu.common.WarehouseConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class HdfsClient implements Closeable {

    private final FileSystem fileSystem;

    public HdfsClient(WarehouseConfig config) {
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", config.get("hdfs.uri"));
            configuration.set("dfs.client.use.datanode.hostname", "true");
            configuration.set("dfs.replication", "1");
            this.fileSystem = FileSystem.get(new URI(config.get("hdfs.uri")), configuration, config.get("hdfs.user"));
        } catch (IOException | InterruptedException | URISyntaxException e) {
            throw new IllegalStateException("Failed to initialize HDFS client", e);
        }
    }

    public boolean exists(String path) throws IOException {
        return fileSystem.exists(new Path(path));
    }

    public void mkdir(String path) throws IOException {
        Path target = new Path(path);
        if (!fileSystem.exists(target)) {
            fileSystem.mkdirs(target);
        }
    }

    public void upload(String localPath, String hdfsPath, boolean overwrite) throws IOException {
        Path source = new Path(localPath);
        Path target = new Path(hdfsPath);
        if (overwrite && fileSystem.exists(target)) {
            fileSystem.delete(target, true);
        }
        fileSystem.copyFromLocalFile(false, true, source, target);
    }

    public List<String> list(String path) throws IOException {
        List<String> result = new ArrayList<String>();
        FileStatus[] statuses = fileSystem.listStatus(new Path(path));
        if (statuses == null) {
            return result;
        }
        for (FileStatus status : statuses) {
            result.add(status.getPath().toString());
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        fileSystem.close();
    }
}
