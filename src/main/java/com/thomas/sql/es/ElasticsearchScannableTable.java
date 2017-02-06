package com.thomas.sql.es;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable2;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.schema.ScannableTable;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * simple table implementation.
 */
public class ElasticsearchScannableTable extends ElasticsearchTable implements ScannableTable
{
    public ElasticsearchScannableTable(TransportClient client, String index, String type)
    {
        super(client, index, type);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return new AbstractEnumerable2<Object[]>() {
            @Override
            public Iterator<Object[]> iterator() {
                SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index);
                SearchResponse searchResponse = searchRequestBuilder.setTypes(type).get();
                List<Object[]> resultList = new ArrayList<>();
                List<String> fieldNames = rowType.getFieldNames();
                for(SearchHit hit : searchResponse.getHits().getHits())
                {
                    Map<String, Object> source = hit.getSource();
                    List<Object> objects = new ArrayList<>(fieldNames.size());
                    for(String fieldName : fieldNames)
                        objects.add(source.get(fieldName));
                    resultList.add(objects.toArray());
                }
                return resultList.iterator();
            }
        };
    }
}
