package org.apache.flink.streaming.connectors.rabbitmq.config;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.rabbitmq.utils.JsonUtil;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;


public class JsonSimpleSchema implements DeserializationSchema<RowData>, SerializationSchema<RowData> {

    private static final long serialVersionUID = -1L;
    private static final Logger LOGGER =
            LoggerFactory.getLogger(JsonSimpleSchema.class);

    private transient TableSchema tableSchema;
    private final int columnSize;
    private final ByteSerializer.ValueType[] fieldTypes;
    private final DataType[] fieldDataTypes;
    private final String[] fieldNames;


    public JsonSimpleSchema(TableSchema tableSchema){
        this.tableSchema = tableSchema;
        this.columnSize = tableSchema.getFieldNames().length;
        this.fieldTypes = new ByteSerializer.ValueType[columnSize];
        this.fieldNames = tableSchema.getFieldNames();
        this.fieldDataTypes = tableSchema.getFieldDataTypes();
        for (int index = 0; index < columnSize; index++) {
            ByteSerializer.ValueType valueType =
                    ByteSerializer.getTypeIndex(tableSchema.getFieldTypes()[index].getTypeClass());
            this.fieldTypes[index] = valueType;
        }
    }

    @Override
    public RowData deserialize(byte[] bytes) throws IOException {
        String content = new String(bytes, StandardCharsets.UTF_8);
        Map<String, Object> map = JsonUtil.objectToMap(content);
        GenericRowData rowData = buildRowData(map);
        return rowData;
    }

    @Override
    public boolean isEndOfStream(RowData data) {
        return false;
    }

    @Override
    public byte[] serialize(RowData data) {
        Map<String, Object> map = rowData2Map(data);
        return JsonUtil.toJson(map).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return InternalTypeInfo.of((RowType) tableSchema.toRowDataType().getLogicalType());
    }


    private GenericRowData buildRowData(Map<String, Object> map){
        GenericRowData rowData = new GenericRowData(columnSize);
        for (int index = 0; index < columnSize; index++) {
            String value = map.get(fieldNames[index]).toString();
            Object deserialize = StringSerializer.deserialize(
                    value,
                    fieldTypes[index],
                    fieldDataTypes[index],
                    new HashSet<>());
            rowData.setField(index,deserialize);
        }
        return rowData;
    }


    //将RowData数据转换为map
    private Map<String,Object> rowData2Map(RowData rowData){
        int arity = rowData.getArity();
        Map<String, Object> map = new LinkedHashMap<>((arity << 2) / 3);
        for (int i = 0; i < arity; i++) {
            String key = fieldNames[i];
            map.put(key,
                    RowData.createFieldGetter(fieldDataTypes[i].getLogicalType(),i)
                            .getFieldOrNull(rowData).toString());
        }
        return map;
    }
}
