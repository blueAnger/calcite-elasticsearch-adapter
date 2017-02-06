package com.thomas.sql.es.rule;

import com.thomas.sql.es.ElasticsearchRelNode;
import com.thomas.sql.es.ElasticsearchTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.elasticsearch.search.sort.SortOrder;

import java.util.List;

public class ElasticsearchSort extends Sort implements ElasticsearchRelNode
{
    public ElasticsearchSort(RelOptCluster cluster, RelTraitSet traits, RelNode child,
                             RelCollation collation, RexNode offset, RexNode fetch)
    {
        super(cluster, traits, child, collation, offset, fetch);
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch)
    {
        return new ElasticsearchSort(getCluster(), traitSet, newInput, collation, offset, fetch);
    }

    @Override
    public void implement(Implementor implementor)
    {
        implementor.visitChild(0, getInput());
        ElasticsearchTable esTable = implementor.getElasticsearchTable();

        List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
        if(fieldCollations != null)
        {
            List<RelDataTypeField> fieldList = esTable.getRowType().getFieldList();
            for(RelFieldCollation fieldCollation : fieldCollations) {
                SortOrder order = SortOrder.ASC;
                switch (fieldCollation.getDirection())
                {
                    case DESCENDING:
                    case STRICTLY_DESCENDING:
                        order = SortOrder.DESC;
                }
                esTable.addSortBuilder(fieldList.get(fieldCollation.getFieldIndex()).getName().toLowerCase(), order);
            }
        }

        if(offset != null && offset instanceof RexLiteral)
            esTable.setSearchOffset(Integer.parseInt(((RexLiteral) offset).getValue2().toString()));
        if(fetch != null && fetch instanceof RexLiteral)
            esTable.setSearchSize(Integer.parseInt(((RexLiteral) fetch).getValue2().toString()));
    }

}
