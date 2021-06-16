package org.apache.flink.formats.protobuf.typeutils;

public class FieldUtil {

    /** Field name of class ( {@link org.apache.flink.formats.protobuf.message.Message} ). */
    public static class MessageV1 {
        public static final String log_timestamp = "log_timestamp";
        public static final String ip = "ip";
        public static final String field = "field";
    }

    /** Field name of class ( {@link org.apache.flink.formats.protobuf.message.NewMessageV3} ). */
    public static class MessageV3 {
        public static final String protocol_version = "protocol_version";
        public static final String log_id = "log_id";
        public static final String session_id = "session_id";
        public static final String event_time = "event_time";
        public static final String client_ip = "client_ip";
        public static final String element = "element";
        public static final String app = "app";
        public static final String page = "page";
        public static final String module = "module";
        public static final String action = "action";
        public static final String referer = "referer";
        public static final String os = "os";
        public static final String device = "device";
        public static final String distinct_id = "distinct_id";
        public static final String tk = "tk";
        public static final String mid = "mid";
        public static final String uuid = "uuid";
        public static final String app_version = "app_version";
        public static final String os_version = "os_version";
        public static final String device_mode = "device_mode";
        public static final String device_manu = "device_manu";
        public static final String os_code = "os_code";
        public static final String device_brand = "device_brand";
        public static final String screen_width = "screen_width";
        public static final String screen_height = "screen_height";
        public static final String device_carrier = "device_carrier";
        public static final String dtu = "dtu";
        public static final String network = "network";
        public static final String vest_name = "vest_name";
        public static final String env = "env";
        public static final String lat = "lat";
        public static final String lon = "lon";
        public static final String app_subversion = "app_subversion";
        public static final String topic = "topic";
        public static final String arrive_time = "arrive_time";
        public static final String event = "event";
        public static final String android_id = "android_id";
        public static final String tuid = "tuid";
        public static final String platform = "platform";
        public static final String ref_module_id = "ref_module_id";
        public static final String refer_event_id = "refer_event_id";
        public static final String screen_size = "screen_size";
        public static final String screen_resolution = "screen_resolution";
        public static final String nm = "nm";
        public static final String bundle_id = "bundle_id";
        public static final String device_uuid = "device_uuid";
        public static final String mp_system_info = "mp_system_info";
        public static final String wx_open_id = "wx_open_id";
        public static final String user_agent = "user_agent";
        public static final String extend_info = "extend_info";
    }
}
