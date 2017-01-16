package com.thomas.sql.es;

import org.apache.calcite.plan.RelOptRule;

import java.util.ArrayList;
import java.util.List;

public class ElasticsearchRules
{
    public static <T extends RelOptRule> List<T> createRules(String ... rules)
    {
        if(rules == null || rules.length == 0)
            rules = new String[]{RuleType.ALL.toString()};

        List<T> resultList = new ArrayList<>(rules.length);
        for(String rule : rules)
            resultList.add(createRule(RuleType.valueOf(rule.toUpperCase())));
        return resultList;
    }

    @SuppressWarnings("unchecked")
    private static <T extends RelOptRule> T createRule(RuleType ruleType)
    {
        switch (ruleType)
        {
            case FILTER:
                return (T) new ElasticsearchFilterRule();
            case PROJECT:
                return (T) new ElasticsearchProjectRule();
            case AGGREGATE:
                return (T) new ElasticsearchAggregateRule();
            default:break;
        }
        return null;
    }

    enum RuleType
    {
        ALL,
        FILTER,
        SORT,
        PROJECT,
        AGGREGATE
    }
}
