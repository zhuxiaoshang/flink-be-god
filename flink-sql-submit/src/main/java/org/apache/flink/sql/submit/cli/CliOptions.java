package org.apache.flink.sql.submit.cli;

/**
 * @author: zhushang
 * @create: 2020-11-05 17:05
 */
public class CliOptions {

    private final String sqlFilePath;
    private final String workingSpace;
    private final String jobName;
    private final String[] udfUrls;

    public CliOptions(String sqlFilePath, String workingSpace, String jobName, String[] udfUrls) {
        this.sqlFilePath = sqlFilePath;
        this.workingSpace = workingSpace;
        this.jobName = jobName;
        this.udfUrls = udfUrls;
    }

    public String getSqlFilePath() {
        return sqlFilePath;
    }

    public String getWorkingSpace() {
        return workingSpace;
    }

    public String[] getUdfUrls() {
        return udfUrls;
    }

    public String getJobName() {
        return jobName;
    }
}
