package com.thomas.sql.es;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.thomas.sql.es.util.ElasticsearchUtil;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ElasticsearchSchema extends AbstractSchema
{
    private TransportClient client;
    private String index;

    public ElasticsearchSchema(String cluster, Map<String, Integer> locations, String index)
    {
        this.index = index;
        client = ElasticsearchUtil.createClient(Collections.singletonMap("cluster.name", cluster), locations);
    }

    @Override
    protected Map<String, Table> getTableMap()
    {
        final Map<String, Table> tableMap = new HashMap<>();
        GetMappingsResponse getMappingsResponse = client.admin().indices().prepareGetMappings(index).get();
        ImmutableOpenMap<String, MappingMetaData> mapping = getMappingsResponse.getMappings().get(index);
        for(ObjectObjectCursor<String, MappingMetaData> c : mapping)
            tableMap.put(c.key.toUpperCase(), new ElasticsearchTranslatableTable(client, index, c.key));
        return tableMap;
    }

}
