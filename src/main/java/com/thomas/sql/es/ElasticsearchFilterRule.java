package com.thomas.sql.es;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;

/**
 * rule which turns {@link LogicalFilter} to {@link ElasticsearchFilter}
 */
public class ElasticsearchFilterRule extends ConverterRule
{
    public ElasticsearchFilterRule()
    {
        super(LogicalFilter.class, Convention.NONE, ElasticsearchRelNode.CONVENTION,
                ElasticsearchFilterRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode rel)
    {
        //rel是匹配成功的子树的根结点，此处是LogicalFilter
        LogicalFilter filter = (LogicalFilter) rel;
        RelTraitSet traitSet = filter.getTraitSet().replace(getOutTrait());
        //ElasticsearchFilter构造器的第3个参数是子结点，即input
        //这里先把原LogicalFilter的input进行convert之后，再构造ElasticsearchFilter实例
        return new ElasticsearchFilter(rel.getCluster(), traitSet,
                convert(filter.getInput(), getOutTrait()), filter.getCondition());
    }

}
