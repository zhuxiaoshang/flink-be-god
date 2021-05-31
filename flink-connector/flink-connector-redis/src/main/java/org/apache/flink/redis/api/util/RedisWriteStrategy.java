package org.apache.flink.redis.api.util;

public enum RedisWriteStrategy {
    WRITE_IF_NOT_EXIST("if_not_exist", "key不存在时写入"),
    ALWAYS("always", "覆盖");

    private String code;

    private String desc;

    RedisWriteStrategy(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public String getCode() {
        return code;
    }

    public static RedisWriteStrategy get(String code) {
        switch (code) {
            case "if_not_exist":
                return WRITE_IF_NOT_EXIST;
            case "always":
                return ALWAYS;
            default:
                throw new RuntimeException("unknow redis write strategy. code ['" + code + "']");
        }
    }
}
