package flink.sql.submit.utils;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.GetObjectRequest;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: zhushang
 * @create: 2021-05-31 11:45
 */
public class OSSUtils {
    public static final String ENDPOINT = "";
    public static final String ACCESSKEYID = "";
    public static final String ACCESSKEYSECRET = "";
    public static final String BUCKETNAME = "";
    public static final String LOCLAPATH = "/";

    public static List<String> downloadFile(String jobName, String... objectName) {
        List<String> urls = new ArrayList<>();
        OSS ossClient = new OSSClientBuilder().build(ENDPOINT, ACCESSKEYID, ACCESSKEYSECRET);
        checkAndCreate(LOCLAPATH + jobName);
        for (String object : objectName) {
            if (!object.endsWith(".jar")) continue;
            String[] arr = object.split("\\/");
            String url = LOCLAPATH + jobName + "/" + arr[arr.length - 1];
            ossClient.getObject(new GetObjectRequest(BUCKETNAME, object), new File(url));
            urls.add("file://" + url);
        }
        ossClient.shutdown();
        return urls;
    }

    private static void checkAndCreate(String s) {
        File f = new File(s);
        if (!f.exists()) {
            f.mkdir();
        }
    }
}
