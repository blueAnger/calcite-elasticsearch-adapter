package com.thomas.sql.es;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;

public class ElasticsearchTable extends AbstractTable
{
    private static Map<Class, Function<String, Object>> CONVERTERS = new HashMap<>();
    static {
        CONVERTERS.put(String.class, s -> s);
        CONVERTERS.put(Integer.class, Integer::parseInt);
        CONVERTERS.put(Float.class, Float::parseFloat);
        CONVERTERS.put(Double.class, Double::parseDouble);
        CONVERTERS.put(Boolean.class, Boolean::parseBoolean);
        CONVERTERS.put(Short.class, Short::parseShort);
        CONVERTERS.put(Byte.class, Byte::parseByte);
        CONVERTERS.put(Long.class, Long::parseLong);
    }

    protected TransportClient client;
    protected String index;
    protected String type;
    protected RelDataType rowType;
    protected final List<QueryBuilder> queryBuilderList = new ArrayList<>();
    protected final List<Pair<AggregationBuilder, Class>> aggregationBuilderList = new ArrayList<>();
    protected final List<SortBuilder> sortBuilderList = new ArrayList<>();
    protected int searchOffset, searchSize;

    public void setSearchOffset(int searchOffset)
    {
        this.searchOffset = searchOffset;
    }

    public void setSearchSize(int searchSize)
    {
        this.searchSize = searchSize;
    }

    public void addQueryBuilder(QueryBuilder queryBuilder)
    {
        queryBuilderList.add(queryBuilder);
    }

    public void addAggregationBuilder(AggregationBuilder aggregationBuilder, Class cls)
    {
        aggregationBuilderList.add(Pair.of(aggregationBuilder, cls));
    }

    public void addSortBuilder(String field, SortOrder order)
    {
        sortBuilderList.add(SortBuilders.fieldSort(field).order(order));
    }

    public ElasticsearchTable(TransportClient client, String index, String type) {
        this.client = client;
        this.index = index;
        this.type = type;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if(rowType == null)
            try {
                rowType = getRowTypeFromEs(typeFactory);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        return rowType;
    }

    public RelDataType getRowType()
    {
        return rowType;
    }

    /**
     * parse elasticsearch mapping to get RelDataType
     * @param typeFactory
     * @return RelDataType instance representing the "table schema"
     * @throws IOException throws when something wrong with json parsing
     */
    @SuppressWarnings("unchecked")
    public RelDataType getRowTypeFromEs(RelDataTypeFactory typeFactory) throws IOException
    {
        RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
        GetMappingsResponse getMappingsResponse = client.admin().indices().prepareGetMappings(index).get();
        MappingMetaData typeMapping = getMappingsResponse.getMappings().get(index).get(type);
        //{"person":{"properties":{"age":{"type":"integer"},"name":{"type":"text","fields":{"raw":{"type":"keyword"}}}}}}
        String json = typeMapping.source().string();
        Map<String, Object> map = new ObjectMapper().readValue(json, new TypeReference<Map<String, Object>>() {});
        Map<String, Object> properties = ((Map<String, Map<String, Object>>) map.get(type)).get("properties");
        int index = 0;
        //(base-field-name, field-map)
        Stack<Pair<String, Map<String, Object>>> mapStack = new Stack<>();
        mapStack.push(Pair.of(null, properties));
        while (!mapStack.isEmpty())
        {
            Pair<String, Map<String, Object>> pair = mapStack.pop();
            String baseFieldName = pair.left;
            for(Map.Entry<String, Object> entry : pair.right.entrySet())
            {
                String name = entry.getKey().toUpperCase();
                if(baseFieldName != null) name = baseFieldName + "." + name;

                Map<String, Object> fieldMap = (Map<String, Object>) entry.getValue();
                String type = fieldMap.get("type") != null ? fieldMap.get("type").toString() : null;
                if(type == null) throw new IllegalStateException(String.format("type of elasticsearch field '%s' is null", name));
                builder.add(new RelDataTypeFieldImpl(name, index++, typeFactory.createJavaType(ES2JavaTypeConverter.toJavaType(type))));

                //multi-field, that means containing 'fields' attribute
                Map<String, Object> moreFields = fieldMap.get("fields") != null ? (Map<String, Object>) fieldMap.get("fields") : null;
                if(moreFields != null) mapStack.push(Pair.of(name, moreFields));
            }
        }
        return builder.build();
    }

    public List<Object[]> search()
    {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);
        if(queryBuilderList.size() > 0)
        {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            for (QueryBuilder queryBuilder : queryBuilderList)
                boolQueryBuilder.must(queryBuilder);
            searchRequestBuilder.setQuery(boolQueryBuilder);
        }
        for(Pair<AggregationBuilder, Class> pair : aggregationBuilderList)
            searchRequestBuilder.addAggregation(pair.left);
        for(SortBuilder sortBuilder : sortBuilderList)
            searchRequestBuilder.addSort(sortBuilder);
        if(searchOffset >= 0)
            searchRequestBuilder.setFrom(searchOffset);
        if(searchSize > 0)
            searchRequestBuilder.setSize(searchSize);

        SearchResponse searchResponse = searchRequestBuilder.get();
        List<Object[]> resultList = new ArrayList<>();
        //aggregation, only one row to return
        if(aggregationBuilderList.size() > 0)
        {
            List<Object> rowObj = new ArrayList<>();
            for(Pair<AggregationBuilder, Class> pair : aggregationBuilderList)
            {
                String name = pair.left.getName();
                NumericMetricsAggregation.SingleValue singleValue = searchResponse.getAggregations().get(name);
                double remain = singleValue.value() % 1;
                if(remain == 0)
                    rowObj.add(CONVERTERS.getOrDefault(pair.right, s -> s).apply(String.valueOf((long)singleValue.value())));
                else rowObj.add(CONVERTERS.getOrDefault(pair.right, s -> s).apply(singleValue.getValueAsString()));
            }
            resultList.add(rowObj.toArray());
        }
        else
        {
            for (SearchHit hit : searchResponse.getHits().getHits())
            {
                Map<String, Object> valueMap = hit.getSource();
                List<RelDataTypeField> fieldList = rowType.getFieldList();
                List<Object> rowObj = new ArrayList<>();
                for(RelDataTypeField field : fieldList)
//                    rowObj.add(convert(field.getType(), valueMap.get(field.getName().toLowerCase()).toString()));
                //considering multi-field
                {
                    Object obj = valueMap.get(field.getName().toLowerCase());
                    if(obj != null)
                        rowObj.add(convert(field.getType(), obj.toString()));
                    else rowObj.add(null);
                }
                if(rowObj.size() > 0)
                    resultList.add(rowObj.toArray());
            }
        }
        return resultList;
    }

    /**
     * convert original text according to the specific Class type
     */
    private Object convert(RelDataType dataType, String originValue)
    {
        if(! (dataType instanceof RelDataTypeFactoryImpl.JavaType))
            return originValue;
        Class javaClass = ((RelDataTypeFactoryImpl.JavaType) dataType).getJavaClass();
        return CONVERTERS.getOrDefault(javaClass, s -> s).apply(originValue);
    }

}
