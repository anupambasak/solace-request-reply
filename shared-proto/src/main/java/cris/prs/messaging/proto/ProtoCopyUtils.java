package cris.prs.messaging.proto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeParseException;

@Service
public class ProtoCopyUtils {

    private final ObjectMapper objectMapper;

    public ProtoCopyUtils() {
        this.objectMapper = getProtoObjectMapper();
    }


    private ObjectMapper getProtoObjectMapper(){
        SimpleModule protoLocalDateModule = new SimpleModule("ProtoLocalDate");
        protoLocalDateModule.addSerializer(LocalDate.class, new JsonSerializer<>() {
            @Override
            public void serialize(LocalDate d, JsonGenerator g, SerializerProvider p) throws IOException {
                g.writeStartObject();
                g.writeNumberField("year", d.getYear());
                g.writeNumberField("month", d.getMonthValue());
                g.writeNumberField("day", d.getDayOfMonth());
                g.writeEndObject();
            }
        });
        protoLocalDateModule.addDeserializer(LocalDate.class, new StdDeserializer<>(LocalDate.class) {
            @Override
            public LocalDate deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
                JsonNode n = p.getCodec().readTree(p);
                if (n.isNull()) return null;
                if (n.isObject()) {
                    return LocalDate.of(n.path("year").asInt(), n.path("month").asInt(), n.path("day").asInt());
                }
                if (n.isTextual()) return LocalDate.parse(n.asText());
                if (n.isArray() && n.size() == 3) {
                    return LocalDate.of(n.get(0).asInt(), n.get(1).asInt(), n.get(2).asInt());
                }
                return (LocalDate) ctxt.handleUnexpectedToken(LocalDate.class, p);
            }
        });

        SimpleModule protoLocalDateTimeModule = new SimpleModule("ProtoLocalDateTime");
        protoLocalDateTimeModule.addSerializer(LocalDateTime.class, new JsonSerializer<>() {

            @Override
            public void serialize(LocalDateTime v, JsonGenerator g, SerializerProvider p) throws IOException {
                g.writeString(v.atZone(ZoneId.of("Asia/Kolkata")).toInstant().toString());
            }
        });
        protoLocalDateTimeModule.addDeserializer(LocalDateTime.class, new StdDeserializer<>(LocalDateTime.class) {
            private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

            @Override
            public LocalDateTime deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
                JsonNode n = p.getCodec().readTree(p);
                if (n.isNull()) return null;
                if (n.isTextual()) {
                    String s = n.asText();
                    try {
                        // Timestamp from JsonFormat: has Z or an offset -> convert to IST
                        return OffsetDateTime.parse(s).atZoneSameInstant(IST).toLocalDateTime();
                    } catch (DateTimeParseException e) {
                        // Plain local value, e.g. "2025-11-19T10:58:44.766907509"
                        return LocalDateTime.parse(s);
                    }
                }
                if (n.isObject() && n.has("seconds")) { // defensive: raw {seconds, nanos}
                    return LocalDateTime.ofInstant(
                            Instant.ofEpochSecond(n.get("seconds").asLong(), n.path("nanos").asLong()), IST);
                }
                return (LocalDateTime) ctxt.handleUnexpectedToken(LocalDateTime.class, p);
            }
        });

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.registerModule(protoLocalDateModule);
        objectMapper.registerModule(protoLocalDateTimeModule);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .registerModule(new SimpleModule()
                        .setSerializerModifier(new BeanSerializerModifier() {
                            @Override
                            public JsonSerializer<?> modifyEnumSerializer(SerializationConfig config, JavaType type,
                                                                          BeanDescription desc, JsonSerializer<?> ser) {
                                String prefix = toUpperSnake(type.getRawClass().getSimpleName()) + "_";
                                return new JsonSerializer<Enum<?>>() {
                                    @Override
                                    public void serialize(Enum<?> v, JsonGenerator g, SerializerProvider p) throws IOException {
                                        g.writeString(prefix + v.name());
                                    }
                                };
                            }
                        })
                        .setDeserializerModifier(new BeanDeserializerModifier() {
                            @Override
                            @SuppressWarnings({"unchecked", "rawtypes"})
                            public JsonDeserializer<?> modifyEnumDeserializer(DeserializationConfig config, JavaType type,
                                                                              BeanDescription desc, JsonDeserializer<?> deser) {
                                final Class<? extends Enum> enumClass = (Class<? extends Enum>) type.getRawClass();
                                final String prefix = toUpperSnake(enumClass.getSimpleName()) + "_";
                                return new JsonDeserializer<Enum<?>>() {
                                    @Override
                                    public Enum<?> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
                                        String s = p.getValueAsString();
                                        if (s == null) return null;
                                        if (s.startsWith(prefix)) s = s.substring(prefix.length());
                                        if (s.equals("UNSPECIFIED")) return null;   // proto zero value, no Java counterpart
                                        try {
                                            return Enum.valueOf((Class) enumClass, s);
                                        } catch (IllegalArgumentException e) {
                                            return (Enum<?>) ctxt.handleWeirdStringValue(enumClass, s, "not a valid enum value");
                                        }
                                    }
                                };
                            }
                        }));
        return objectMapper;
    }

    static String toUpperSnake(String s) {
        return s.replaceAll("([a-z0-9])([A-Z])", "$1_$2")
                .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
                .toUpperCase();
    }

    public <T> T toDto(Message proto, Class<T> clz) throws InvalidProtocolBufferException, JsonProcessingException {
        String j = JsonFormat.printer().print(proto);
        return objectMapper.readValue(j,clz);
    }

    public Message toProto(Object obj, Message.Builder builder) throws JsonProcessingException, InvalidProtocolBufferException {
        String dtoJson = objectMapper.writeValueAsString(obj);
        JsonFormat.parser().ignoringUnknownFields().merge(dtoJson, builder);
        return builder.build();
    }
}
