package demo;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @Description : TODO
 * @author: lichong
 * @date: 2023/6/7
 * @version: 1.0
 */
public class HadoopUtils {

    private static final String CORE_SITE_LOCATION = "file:///etc/hadoop/conf/core-site.xml";
    private static final String HDFS_SITE_LOCATION = "file:///etc/hadoop/conf/hdfs-site.xml";
    private Configuration conf;
    private FileSystem fs;

    public HadoopUtils(String path) {
        Logger logger = LoggerFactory.getLogger(HadoopUtils.class);
        try {
            conf = new Configuration();
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", LocalFileSystem.class.getName());
            conf.setBoolean("fs.hdfs.impl.disable.cache", true);
            conf.addResource(new Path(CORE_SITE_LOCATION));
            conf.addResource(new Path(HDFS_SITE_LOCATION));
            UserGroupInformation.setConfiguration(conf);
            if (StringUtils.isNotBlank(path)) {
                String[] pathArr = path.split("/");
                String updateDefaultFS = pathArr[0] + "//" + pathArr[2];
                conf.set("fs.defaultFS", updateDefaultFS);
                logger.info("选择的DefaultFs:" + updateDefaultFS);
            }
            String defaultFS = conf.get("fs.defaultFS");
            //兼容跨集群
            if (defaultFS.startsWith("viewfs://")) {
                fs = ViewFileSystem.get(conf);
            } else {
                fs = FileSystem.get(conf);
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * 根据hdfs路径获得路径的状态
     *
     * @param hdfsPath
     * @return
     * @throws IOException
     */
    public int getFileStatus(String hdfsPath) throws IOException {
        FileStatus[] fileStatus = fs.globStatus(new Path(hdfsPath));
        if (fileStatus == null || fileStatus.length == 0) {
            throw new FileNotFoundException("Cannot access " + hdfsPath + ": No such file or directory.");
        }
        int num = 0;
        for (FileStatus status : fileStatus) {
            num += getFileStatus(status);
        }
        return num;
    }

    private int getFileStatus(FileStatus fileStatus) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(fileStatus.getPath());
        if (fileStatuses.length == 1 && !fileStatuses[0].isDirectory()) {
            return 1;
        }
        int num = 0;
        for (FileStatus status : fileStatuses) {
            num += getFileStatus(status);
        }
        return num;
    }

    /**
     * 根据hdfsPath和Path过滤器来获得子路径的状态
     *
     * @param hdfsPath
     * @param pathFilter
     * @return
     * @throws IOException
     */
    public List<FileStatus> getChildren(String hdfsPath, PathFilter pathFilter) throws IOException {
        List<FileStatus> retList = new ArrayList<>();
        FileStatus[] fileStatuses = fs.globStatus(new Path(hdfsPath), pathFilter);
        for (FileStatus fileStatus : fileStatuses) {
            List<FileStatus> list = getChildren(fileStatus, pathFilter);
            retList.addAll(list);
        }
        return retList;
    }

    private List<FileStatus> getChildren(FileStatus fileStatus, PathFilter pathFilter) throws IOException {
        List<FileStatus> retList = new ArrayList<>();
        if (fileStatus.getPath().toString().contains("_temporary")) {
            return retList;
        }
        FileStatus[] fileStatuses = fs.listStatus(fileStatus.getPath(), pathFilter);
        if (fileStatuses.length == 1 && !fileStatuses[0].isDirectory()) {
            return Collections.singletonList(fileStatuses[0]);
        }
        for (FileStatus status : fileStatuses) {
            List<FileStatus> list = getChildren(status, pathFilter);
            retList.addAll(list);
        }
        return retList;
    }

    /**
     * 判断hdfs路径是否是目录
     *
     * @param hdfsPath
     * @return
     * @throws IOException
     */
    public boolean isDirectory(String hdfsPath) throws IOException {
        int num = getFileStatus(hdfsPath);
        return num != 1;
    }

    /**
     * 根据hdfsPath和过滤的关键字来获得子路径
     *
     * @param hdfsPath
     * @param filterFileNames
     * @param filterFilePathNames
     * @return
     * @throws IOException
     */
    public List<String> getChildrenPath(String hdfsPath, final String[] filterFileNames, final String[] filterFilePathNames) throws IOException {
        PathFilter pathFilter = path -> {
            if (filterFileNames != null && filterFileNames.length != 0) {
                for (String filterFileName : filterFileNames) {
                    if (path.getName().equals(filterFileName)) {
                        return false;
                    }
                }
            }
            if (filterFilePathNames != null && filterFilePathNames.length != 0) {
                for (String filterFilePathName : filterFilePathNames) {
                    if (path.getParent().getName().equals(filterFilePathName)) {
                        return false;
                    }
                }
            }
            return true;
        };
        List<FileStatus> list = getChildren(hdfsPath, pathFilter);
        List<String> children = new ArrayList<>();
        for (FileStatus fileStatus : list) {
            if (fileStatus.isDirectory()) {
                continue;
            }
            String path = fileStatus.getPath().toUri().toString();
            children.add(path);
        }
        return children;
    }

    public void closeResource() throws Exception {
        if (fs != null) {
            fs.close();
        }
    }

    public FileSystem getFs() {
        return fs;
    }

    public Configuration getConf() {
        return conf;
    }

}
