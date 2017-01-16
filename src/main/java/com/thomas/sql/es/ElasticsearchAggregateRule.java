package com.thomas.sql.es;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;

public class ElasticsearchAggregateRule extends ConverterRule
{
    public ElasticsearchAggregateRule()
    {
        super(LogicalAggregate.class, Convention.NONE, ElasticsearchRelNode.CONVENTION,
                ElasticsearchAggregateRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode rel)
    {
        LogicalAggregate aggregate = (LogicalAggregate) rel;
        RelTraitSet traitSet = aggregate.getTraitSet().replace(getOutTrait());
        for (AggregateCall call : aggregate.getAggCallList())
        {
            switch (call.getAggregation().getKind())
            {
                case MIN:
                case MAX:
                case COUNT:
                case SUM:
                case AVG:break;
                default:return null;//doesn't match. aggregate rule doesn't fire
            }
        }
        return new ElasticsearchAggregate(aggregate.getCluster(), traitSet,
                convert(aggregate.getInput(), getOutTrait()), aggregate.indicator,
                aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList());
    }
}
