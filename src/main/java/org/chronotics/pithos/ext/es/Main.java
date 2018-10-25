package org.chronotics.pithos.ext.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.chronotics.pithos.ext.es.adaptor.ElasticConnection;
import org.chronotics.pithos.ext.es.model.ESFilterAllRequestModel;
import org.chronotics.pithos.ext.es.model.ESPrepAbstractModel;
import org.chronotics.pithos.ext.es.model.ESPrepListActionRequestModel;
import org.chronotics.pithos.ext.es.util.ESPrepActionConverterUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Main {
    private static ElasticConnection objESConnection;

    public static void main(String[] args) throws IOException {
        objESConnection = ElasticConnection.getInstance("docker-cluster",
                "192.168.0.74", 9304);
        System.setProperty("log4j.configurationFile","C:\\Program Files\\Git\\code\\git\\chron-prep-was\\src\\main\\resources\\logback.xml");

        ObjectMapper objectMapper = new ObjectMapper();
        String test = "{\"index\":\"ss_demo_data_4\",\"type\":\"spi\",\"actions\":[{\"action_idx\":1539325491093,\"action_type\":\"DATA_FORMAT\",\"action_id\":\"LowerCase\",\"data_values\":[\"lot_cd\"],\"user_values\":null}]}";
        String test2 = "{\"index\":\"shakespeare\",\"type\":\"doc\",\"actions\":[{\"action_idx\":1539325491093,\"action_type\":\"FIELDS\",\"action_id\":\"REMOVE_FIELD\",\"data_values\":[\"new_text_entry\"],\"user_values\":null}]}";
        String test3 = "{\"index\":\"ss_demo_data_4\",\"type\":\"spi\",\"actions\":[{\"action_idx\":1539325491093,\"action_type\":\"DATA_TYPE_CHANGE\",\"action_id\":\"keyword\",\"data_values\":[\"module_num\"],\"user_values\":null}]}";
        String test4 = "{\n" +
                "  \"index\": \"shakespeare\",\n" +
                "  \"type\": \"doc\",\n" +
                "  \"actions\": [\n" +
                "    {\n" +
                "      \"action_idx\": 1539325491093,\n" +
                "      \"action_type\": \"DATA_FORMAT\",\n" +
                "      \"action_id\": \"UpperCase\",\n" +
                "      \"new_field_name\": \"new_text_entry\",\n" +
                "      \"data_values\": [\n" +
                "        \"text_entry\"\n" +
                "      ],\n" +
                "      \"user_values\": null\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        ESPrepListActionRequestModel objAllPreps = objectMapper.readValue(test2, ESPrepListActionRequestModel.class);
        List<ESPrepAbstractModel> lstPrepOp = ESPrepActionConverterUtil.convert(objAllPreps);
        objESConnection.prepESData(lstPrepOp);

//        String test4 = "{\n" +
//                "  \"filters\": [\n" +
//                "    {\n" +
//                "      \"filtered_on_field\": \"y_offset\",\n" +
//                "      \"filtered_operation\": \"5\",\n" +
//                "      \"filtered_conditions\": [],\n" +
//                "      \"from_range_condition\": \"-0.1\",\n" +
//                "      \"to_range_condition\": \"0\"\n" +
//                "    }\n" +
//                "  ],\n" +
//                "  \"selected_fields\": [\n" +
//                "    \"y_offset\"],\n" +
//                "    \"deleted_rows\": [\"r2xgsmUBtvI6V1Kaqtf5\", \"tmxgsmUBtvI6V1Kaqtf5\", \"vWxgsmUBtvI6V1Kaqtf5\"]\n" +
//                "}";
//        ESFilterAllRequestModel filter = objectMapper.readValue(test4, ESFilterAllRequestModel.class);
//        Map<String, Object> mapSearchResult = objESConnection.searchDataWithFieldIdxAndRowIdx("ss_demo_data_4", "spi", "",
//                new ArrayList<String>(),0, 25, 0, 0, true, filter);
//        System.out.println(objectMapper.writeValueAsString(mapSearchResult));

    }
}
