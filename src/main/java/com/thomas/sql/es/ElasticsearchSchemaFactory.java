package com.thomas.sql.es;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.HashMap;
import java.util.Map;

public class ElasticsearchSchemaFactory implements SchemaFactory
{
    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        String cluster = (String) operand.get("cluster");
        if(cluster == null)
            throw new RuntimeException("missing parameter 'cluster'");
        String index = (String) operand.get("index");
        if(index == null)
            throw new RuntimeException("missing parameter 'index'");
        String addresses = (String) operand.get("addresses");
        if(addresses == null)
            throw new RuntimeException("missing parameter 'addresses'");
        if(!addresses.matches("[^\\s]+:\\s*\\d+(,\\s*[^\\s]+:\\s*\\d+)*"))
            throw new IllegalArgumentException("invalid addresess value: " + addresses);

        addresses = addresses.replaceAll("\\s+", "");
        Map<String, Integer> locations = new HashMap<>();
        for(String location : addresses.split(","))
            locations.put(location.split(":")[0], Integer.parseInt(location.split(":")[1]));

        return new ElasticsearchSchema(cluster, locations, index);
    }
}
