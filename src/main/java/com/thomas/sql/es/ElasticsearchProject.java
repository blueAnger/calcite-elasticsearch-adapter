package com.thomas.sql.es;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class ElasticsearchProject extends Project implements ElasticsearchRelNode
{
    protected ElasticsearchProject(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<? extends RexNode> projects, RelDataType rowType)
    {
        super(cluster, traits, input, projects, rowType);
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType)
    {
        return new ElasticsearchProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public void implement(Implementor implementor)
    {
        implementor.visitChild(0, getInput());

        List<RexNode> projects = getProjects();
        for(RexNode rexNode : projects)
        {
            System.out.println(rexNode.getClass());
        }
    }
}
