package com.thomas.sql.es.util;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

public class ElasticsearchUtil
{
    public static TransportClient createClient(Map<String, String> settingMap, Map<String, Integer> addresses)
    {
        Settings settings = Settings.builder().put(settingMap).build();
        TransportClient client = new PreBuiltTransportClient(settings);

        for (Map.Entry<String, Integer> entry : addresses.entrySet())
            client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(entry.getKey(), entry.getValue())));
        return client;
    }

    public static <T> T normalSearch(SearchRequestBuilder searchRequestBuilder, Function<SearchResponse, T> fun)
    {
        return fun.apply(searchRequestBuilder.get());
    }

    public static <T> Iterator<T> fullSearch(TransportClient client, SearchRequestBuilder searchRequestBuilder,
                                      long keepAliveTimeMillis, int batchSize, Function<SearchResponse, T> fun)
    {
        return new Iterator<T>()
        {
            SearchResponse searchResponse = searchRequestBuilder.setScroll(TimeValue.timeValueMillis(keepAliveTimeMillis)).setSize(batchSize).get();
            @Override
            public boolean hasNext() {
                return searchResponse.getHits().getHits().length > 0;
            }

            @Override
            public T next() {
                T res = fun.apply(searchResponse);
                searchResponse = client.prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMillis(keepAliveTimeMillis)).get();
                return res;
            }
        };
    }
}
