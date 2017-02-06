package com.thomas.sql.es.rule;

import com.thomas.sql.es.ElasticsearchRelNode;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalSort;

import java.util.List;

public class ElasticsearchSortRule extends ConverterRule
{
    public ElasticsearchSortRule()
    {
        super(LogicalSort.class, Convention.NONE, ElasticsearchRelNode.CONVENTION,
                ElasticsearchSortRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode rel)
    {
        LogicalSort sort = (LogicalSort) rel;
        RelCollation collation = sort.getCollation();
        if(collation != null)
        {
            List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
            if(fieldCollations != null && fieldCollations.size() > 0)
                return new ElasticsearchSort(sort.getCluster(), sort.getTraitSet().replace(getOutTrait()),
                        sort.getInput(), collation, sort.offset, sort.fetch);
        }
        return null;
    }
}
