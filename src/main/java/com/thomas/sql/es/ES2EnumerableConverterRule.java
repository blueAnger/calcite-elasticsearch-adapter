package com.thomas.sql.es;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * result set conversion rule which converts {@link ElasticsearchRelNode}
 * to ${@link org.apache.calcite.adapter.enumerable.EnumerableRel}
 */
public class ES2EnumerableConverterRule extends ConverterRule
{
    public ES2EnumerableConverterRule()
    {
        super(RelNode.class, ElasticsearchRelNode.CONVENTION, EnumerableConvention.INSTANCE,
                ES2EnumerableConverterRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode rel)
    {
        RelTraitSet traitSet = rel.getTraitSet().replace(getOutTrait());
        return new ES2EnumerableConverter(rel.getCluster(), traitSet, rel);
    }
}
