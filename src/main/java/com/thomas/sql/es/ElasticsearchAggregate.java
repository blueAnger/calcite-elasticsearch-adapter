package com.thomas.sql.es;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.elasticsearch.search.aggregations.AggregationBuilders;

import java.util.List;

public class ElasticsearchAggregate extends Aggregate implements ElasticsearchRelNode
{
    protected ElasticsearchAggregate(RelOptCluster cluster, RelTraitSet traits, RelNode child,
                                     boolean indicator, ImmutableBitSet groupSet,
                                     List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls)
    {
        super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, boolean indicator,
                          ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls)
    {
        return new ElasticsearchAggregate(getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls);
    }

    @Override
    public void implement(Implementor implementor)
    {
        implementor.visitChild(0, getInput());
        List<AggregateCall> aggCallList = getAggCallList();
        ElasticsearchTable esTable = implementor.elasticsearchTable;
        List<RelDataTypeField> fieldList = esTable.rowType.getFieldList();
        for(AggregateCall call : aggCallList)
        {
            SqlAggFunction function = call.getAggregation();
            List<Integer> argList = call.getArgList();
            String functionName = function.getName();
            switch (function.getKind())
            {
                case MIN:
                    //min值 与 原字段值 的类型是一样的
                    RelDataTypeField typeField = fieldList.get(argList.get(0));
                    esTable.addAggregationBuilder(AggregationBuilders.min(functionName).field(typeField.getName().toLowerCase()),
                            ((RelDataTypeFactoryImpl.JavaType) typeField.getType()).getJavaClass());
                    break;
                case MAX:
                    //max值 与 原字段值 的类型是一样的
                    RelDataTypeField typeField1 = fieldList.get(argList.get(0));
                    esTable.addAggregationBuilder(AggregationBuilders.max(functionName).field(typeField1.getName().toLowerCase()),
                            ((RelDataTypeFactoryImpl.JavaType) typeField1.getType()).getJavaClass());
                    break;
                case COUNT:
                    if(argList == null || argList.size() == 0)//count(*)
                        esTable.addAggregationBuilder(AggregationBuilders.count(functionName), Long.class);
                    else esTable.addAggregationBuilder(AggregationBuilders.count(functionName).field(fieldList.get(argList.get(0)).getName().toLowerCase()), Long.class);
                    break;
                case SUM:
                    esTable.addAggregationBuilder(AggregationBuilders.sum(functionName).field(fieldList.get(argList.get(0)).getName().toLowerCase()), Double.class);
                    break;
                case AVG:
                    esTable.addAggregationBuilder(AggregationBuilders.avg(functionName).field(fieldList.get(argList.get(0)).getName().toLowerCase()), Double.class);
                    break;
                default:break;
            }
        }
    }
}
