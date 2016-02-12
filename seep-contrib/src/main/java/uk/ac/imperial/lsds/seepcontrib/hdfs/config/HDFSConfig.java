package uk.ac.imperial.lsds.seepcontrib.hdfs.config;

import uk.ac.imperial.lsds.seep.config.Config;
import uk.ac.imperial.lsds.seep.config.ConfigDef;
import uk.ac.imperial.lsds.seep.config.ConfigDef.Importance;
import uk.ac.imperial.lsds.seep.config.ConfigDef.Type;

import java.util.Map;

public class HDFSConfig extends Config {

    private static final ConfigDef config;

    public static final String HDFS_DEFAULT_FS = "hdfs.uri";
    private static final String HDFS_DEFAULT_FS_DOC = "A URI containing the hostname, port and path of where to establish the HDFS connection to.";

    static{
        config = new ConfigDef().define(HDFS_DEFAULT_FS, Type.STRING, Importance.HIGH, HDFS_DEFAULT_FS_DOC);
    }

    public HDFSConfig(Map<?, ?> originals) {
        super(config, originals);
    }

    public static void main(String[] args) {
        System.out.println(config.toHtmlTable());
    }
}
