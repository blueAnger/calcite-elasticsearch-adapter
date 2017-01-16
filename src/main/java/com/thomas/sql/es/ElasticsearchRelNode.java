package com.thomas.sql.es;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

public interface ElasticsearchRelNode extends RelNode
{
    void implement(Implementor implementor);

    Convention CONVENTION = new Convention.Impl("ELASTICSEARCH", ElasticsearchRelNode.class);

    /**
     * Callback for the implementation process that converts a tree of
     * {@link ElasticsearchRelNode} nodes into an Elasticsearch query.
     */
    class Implementor
    {
        RelOptTable table;
        ElasticsearchTable elasticsearchTable;

        public void visitChild(int ordinal, RelNode input)
        {
            assert ordinal == 0;
            ((ElasticsearchRelNode) input).implement(this);
        }
    }
}
