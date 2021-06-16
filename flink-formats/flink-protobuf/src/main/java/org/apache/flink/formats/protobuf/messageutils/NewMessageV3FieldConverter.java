package org.apache.flink.formats.protobuf.messageutils;

import com.google.protobuf.Descriptors;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.protobuf.exception.ProtobufException;
import org.apache.flink.formats.protobuf.message.NewMessageV3;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NewMessageV3FieldConverter extends ProtobufFieldConverter {

    private static final long serialVersionUID = -224294630388809195L;

    @Override
    String initializeSchemaString() {
        return "syntax = \"proto3\";\n"
                + "package pbdata;\n"
                + " \n"
                + "message Log {\n"
                + "    string protocol_version  = 1;\n"
                + "    string log_id  = 2;\n"
                + "    string session_id = 3;\n"
                + "    int64 event_time = 4;\n"
                + "    string client_ip = 5;\n"
                + "    string element = 6;\n"
                + "    string app = 7;\n"
                + "    string page = 8;\n"
                + "    string module = 9;\n"
                + "    string action = 10;\n"
                + "    string referer = 11;\n"
                + "    string os = 12;\n"
                + "    string device = 13;\n"
                + "    string distinct_id = 14;\n"
                + "    string tk = 15;\n"
                + "    string mid = 16;\n"
                + "    string uuid = 17;\n"
                + "    string app_version = 18;\n"
                + "    string os_version = 19;\n"
                + "    string device_mode = 20;\n"
                + "    string device_manu = 21;\n"
                + "    int64 os_code = 22;\n"
                + "    string device_brand = 23;\n"
                + "    string screen_width = 24;\n"
                + "    string screen_height = 25;\n"
                + "    string device_carrier = 26;\n"
                + "    string dtu = 27;\n"
                + "    string network = 28;\n"
                + "    string vest_name = 29;\n"
                + "    string env = 30;\n"
                + "    string lat = 31;\n"
                + "    string lon = 32;\n"
                + "    string app_subversion = 33;\n"
                + "    string topic = 34;\n"
                + "    int64 arrive_time = 36;\n"
                + "    string event = 37;\n"
                + "    string android_id = 38;\n"
                + "    string tuid = 39;\n"
                + "    string platform = 40;\n"
                + "    string ref_module_id = 41;\n"
                + "    string refer_event_id = 42;\n"
                + "    string screen_size = 43;\n"
                + "    string screen_resolution = 44;\n"
                + "    string nm = 45;\n"
                + "    string bundle_id = 46;\n"
                + "    string device_uuid = 47;\n"
                + "    string mp_system_info = 48;\n"
                + "    string wx_open_id = 49;\n"
                + "    string user_agent = 50;\n"
                + "    map<string,string> extend_info = 35;\n"
                + "}";
    }

    @Override
    Map<String, Descriptors.FieldDescriptor> initializeFieldDescriptors() {
        List<Descriptors.FieldDescriptor> fields = NewMessageV3.Log.getDescriptor().getFields();
        Map<String, Descriptors.FieldDescriptor> descriptorMap = new HashMap<>(fields.size());
        for (Descriptors.FieldDescriptor field : fields) {
            String name = field.toProto().getName();
            descriptorMap.put(name, field);
        }
        return descriptorMap;
    }

    @Override
    Row convertSchemaToRow(byte[] message) {
        return null;
    }

    @Override
    public Object[] convertSchemaToObjectArray(byte[] message) {
        return createFieldValues(message);
    }

    public NewMessageV3FieldConverter(RowTypeInfo typeInformation, Boolean ignoreParseErrors) {
        super(typeInformation, ignoreParseErrors);
    }

    private Object[] createFieldValues(byte[] message) {
        try {
            NewMessageV3.Log log = NewMessageV3.Log.parseFrom(message);
            Object[] objects = new Object[protoFieldNumbers.length];
            for (int i = 0; i < this.protoFieldNumbers.length; i++) {
                objects[i] = getFieldValue(log, protoFieldNumbers[i]);
            }
            return objects;
        } catch (Exception e) {
            if (!ignoreParseErrors) {
                throw new ProtobufException("parse row exception ", e);
            }
        }
        return null;
    }

    private Object getFieldValue(NewMessageV3.Log log, int columnNumber) throws Exception {
        if (log == null) {
            return null;
        }
        switch (columnNumber) {
            case NewMessageV3.Log.PROTOCOL_VERSION_FIELD_NUMBER:
                return log.hasProtocolVersion() ? log.getProtocolVersion() : null;
            case NewMessageV3.Log.LOG_ID_FIELD_NUMBER:
                return log.hasLogId() ? log.getLogId() : null;
            case NewMessageV3.Log.SESSION_ID_FIELD_NUMBER:
                return log.hasSessionId() ? log.getSessionId() : null;
            case NewMessageV3.Log.EVENT_TIME_FIELD_NUMBER:
                return log.hasEventTime() ? log.getEventTime() : null;
            case NewMessageV3.Log.CLIENT_IP_FIELD_NUMBER:
                return log.hasClientIp() ? log.getClientIp() : null;
            case NewMessageV3.Log.ELEMENT_FIELD_NUMBER:
                return log.hasElement() ? log.getElement() : null;
            case NewMessageV3.Log.APP_FIELD_NUMBER:
                return log.hasApp() ? log.getApp() : null;
            case NewMessageV3.Log.PAGE_FIELD_NUMBER:
                return log.hasPage() ? log.getPage() : null;
            case NewMessageV3.Log.MODULE_FIELD_NUMBER:
                return log.hasModule() ? log.getModule() : null;
            case NewMessageV3.Log.ACTION_FIELD_NUMBER:
                return log.hasAction() ? log.getAction() : null;
            case NewMessageV3.Log.REFERER_FIELD_NUMBER:
                return log.hasReferer() ? log.getReferer() : null;
            case NewMessageV3.Log.OS_FIELD_NUMBER:
                return log.hasOs() ? log.getOs() : null;
            case NewMessageV3.Log.DEVICE_FIELD_NUMBER:
                return log.hasDevice() ? log.getDevice() : null;
            case NewMessageV3.Log.DISTINCT_ID_FIELD_NUMBER:
                return log.hasDistinctId() ? log.getDistinctId() : null;
            case NewMessageV3.Log.TK_FIELD_NUMBER:
                return log.hasTk() ? log.getTk() : null;
            case NewMessageV3.Log.MID_FIELD_NUMBER:
                return log.hasMid() ? log.getMid() : null;
            case NewMessageV3.Log.UUID_FIELD_NUMBER:
                return log.hasUuid() ? log.getUuid() : null;
            case NewMessageV3.Log.APP_VERSION_FIELD_NUMBER:
                return log.hasAppVersion() ? log.getAppVersion() : null;
            case NewMessageV3.Log.OS_VERSION_FIELD_NUMBER:
                return log.hasOsVersion() ? log.getOsVersion() : null;
            case NewMessageV3.Log.DEVICE_MODE_FIELD_NUMBER:
                return log.hasDeviceMode() ? log.getDeviceMode() : null;
            case NewMessageV3.Log.DEVICE_MANU_FIELD_NUMBER:
                return log.hasDeviceManu() ? log.getDeviceManu() : null;
            case NewMessageV3.Log.OS_CODE_FIELD_NUMBER:
                return log.hasOsCode() ? log.getOsCode() : null;
            case NewMessageV3.Log.DEVICE_BRAND_FIELD_NUMBER:
                return log.hasDeviceBrand() ? log.getDeviceBrand() : null;
            case NewMessageV3.Log.SCREEN_WIDTH_FIELD_NUMBER:
                return log.hasScreenWidth() ? log.getScreenWidth() : null;
            case NewMessageV3.Log.SCREEN_HEIGHT_FIELD_NUMBER:
                return log.hasScreenHeight() ? log.getScreenHeight() : null;
            case NewMessageV3.Log.DEVICE_CARRIER_FIELD_NUMBER:
                return log.hasDeviceCarrier() ? log.getDeviceCarrier() : null;
            case NewMessageV3.Log.DTU_FIELD_NUMBER:
                return log.hasDtu() ? log.getDtu() : null;
            case NewMessageV3.Log.NETWORK_FIELD_NUMBER:
                return log.hasNetwork() ? log.getNetwork() : null;
            case NewMessageV3.Log.VEST_NAME_FIELD_NUMBER:
                return log.hasVestName() ? log.getVestName() : null;
            case NewMessageV3.Log.ENV_FIELD_NUMBER:
                return log.hasEnv() ? log.getEnv() : null;
            case NewMessageV3.Log.LAT_FIELD_NUMBER:
                return log.hasLat() ? log.getLat() : null;
            case NewMessageV3.Log.LON_FIELD_NUMBER:
                return log.hasLon() ? log.getLon() : null;
            case NewMessageV3.Log.APP_SUBVERSION_FIELD_NUMBER:
                return log.hasAppSubversion() ? log.getAppSubversion() : null;
            case NewMessageV3.Log.TOPIC_FIELD_NUMBER:
                return log.hasTopic() ? log.getTopic() : null;
            case NewMessageV3.Log.ARRIVE_TIME_FIELD_NUMBER:
                return log.hasArriveTime() ? log.getArriveTime() : null;
            case NewMessageV3.Log.EVENT_FIELD_NUMBER:
                return log.hasEvent() ? log.getEvent() : null;
            case NewMessageV3.Log.ANDROID_ID_FIELD_NUMBER:
                return log.hasAndroidId() ? log.getAndroidId() : null;
            case NewMessageV3.Log.TUID_FIELD_NUMBER:
                return log.hasTuid() ? log.getTuid() : null;
            case NewMessageV3.Log.PLATFORM_FIELD_NUMBER:
                return log.hasPlatform() ? log.getPlatform() : null;
            case NewMessageV3.Log.REF_MODULE_ID_FIELD_NUMBER:
                return log.hasRefModuleId() ? log.getRefModuleId() : null;
            case NewMessageV3.Log.REFER_EVENT_ID_FIELD_NUMBER:
                return log.hasReferEventId() ? log.getReferEventId() : null;
            case NewMessageV3.Log.SCREEN_SIZE_FIELD_NUMBER:
                return log.hasScreenSize() ? log.getScreenSize() : null;
            case NewMessageV3.Log.SCREEN_RESOLUTION_FIELD_NUMBER:
                return log.hasScreenResolution() ? log.getScreenResolution() : null;
            case NewMessageV3.Log.NM_FIELD_NUMBER:
                return log.hasNm() ? log.getNm() : null;
            case NewMessageV3.Log.BUNDLE_ID_FIELD_NUMBER:
                return log.hasBundleId() ? log.getBundleId() : null;
            case NewMessageV3.Log.DEVICE_UUID_FIELD_NUMBER:
                return log.hasDeviceUuid() ? log.getDeviceUuid() : null;
            case NewMessageV3.Log.MP_SYSTEM_INFO_FIELD_NUMBER:
                return log.hasMpSystemInfo() ? log.getMpSystemInfo() : null;
            case NewMessageV3.Log.WX_OPEN_ID_FIELD_NUMBER:
                return log.hasWxOpenId() ? log.getWxOpenId() : null;
            case NewMessageV3.Log.USER_AGENT_FIELD_NUMBER:
                return log.hasUserAgent() ? log.getUserAgent() : null;
            case NewMessageV3.Log.EXTEND_INFO_FIELD_NUMBER:
                return log.getExtendInfoMap();
            default:
                return null;
        }
    }
}
