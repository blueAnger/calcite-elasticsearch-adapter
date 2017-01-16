package com.thomas.sql.es;

import java.util.HashMap;
import java.util.Map;

/**
 * turn elasticsearch field type into java type
 */
public class ES2JavaTypeConverter
{
    public enum ElasticsearchType
    {
        TEXT,
        KEYWORD,
        INTEGER,
        FLOAT,
        DOUBLE,
        BOOLEAN
    }

    private static final Map<ElasticsearchType, Class> TYPE_MAPPING_CACHE =
            new HashMap<>(ElasticsearchType.values().length);

    static {
        for(ElasticsearchType type : ElasticsearchType.values())
        {
            Class cls = String.class;
            switch (type)
            {
                case INTEGER:
                    cls = Integer.class;
                    break;
                case FLOAT:
                    cls = Float.class;
                    break;
                case DOUBLE:
                    cls = Double.class;
                    break;
                case BOOLEAN:
                    cls = Boolean.class;
                    break;
                case TEXT:
                case KEYWORD:
                default:break;
            }
            TYPE_MAPPING_CACHE.put(type, cls);
        }
    }

    public static Class toJavaType(ElasticsearchType type)
    {
        return TYPE_MAPPING_CACHE.get(type);
    }

    public static Class toJavaType(String type)
    {
        ElasticsearchType _type = ElasticsearchType.valueOf(type.toUpperCase());
        return toJavaType(_type);
    }
}
