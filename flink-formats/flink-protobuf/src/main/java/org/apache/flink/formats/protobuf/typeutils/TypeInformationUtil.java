package org.apache.flink.formats.protobuf.typeutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.protobuf.exception.ProtobufException;

import static org.apache.flink.formats.protobuf.typeutils.FieldUtil.MessageV1.*;
import static org.apache.flink.formats.protobuf.typeutils.FieldUtil.MessageV3.*;

public final class TypeInformationUtil {

    public static RowTypeInfo createFullTypeInformationFromMessageV1() {
        return (RowTypeInfo)
                Types.ROW_NAMED(
                        new String[] {log_timestamp, ip, field},
                        new TypeInformation[] {
                            Types.LONG, Types.STRING, Types.MAP(Types.STRING, Types.STRING)
                        });
    }

    public static RowTypeInfo createFullTypeInformationFromMessageV3() {
        return (RowTypeInfo)
                Types.ROW_NAMED(
                        new String[] {
                            protocol_version,
                            log_id,
                            session_id,
                            event_time,
                            client_ip,
                            element,
                            app,
                            page,
                            module,
                            action,
                            referer,
                            os,
                            device,
                            distinct_id,
                            tk,
                            mid,
                            uuid,
                            app_version,
                            os_version,
                            device_mode,
                            device_manu,
                            os_code,
                            device_brand,
                            screen_width,
                            screen_height,
                            device_carrier,
                            dtu,
                            network,
                            vest_name,
                            env,
                            lat,
                            lon,
                            app_subversion,
                            topic,
                            arrive_time,
                            event,
                            android_id,
                            tuid,
                            platform,
                            ref_module_id,
                            refer_event_id,
                            screen_size,
                            screen_resolution,
                            nm,
                            bundle_id,
                            device_uuid,
                            mp_system_info,
                            wx_open_id,
                            user_agent,
                            extend_info
                        },
                        new TypeInformation[] {
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.LONG,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.LONG,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.LONG,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.MAP(Types.STRING, Types.STRING),
                        });
    }

    public static RowTypeInfo of(String messageVersion) {
        MessageVersion mv = MessageVersion.of(messageVersion);
        switch (mv) {
            case MESSAGE_V1:
                return createFullTypeInformationFromMessageV1();
            case MESSAGE_V2:
                return createFullTypeInformationFromMessageV3();
            default:
                throw new ProtobufException(
                        String.format("No fullTypeInformation found for %s", messageVersion));
        }
    }
}
