package com.thomas.sql.es;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * rule which turns {@link LogicalProject} to {@link ElasticsearchProject}
 */
public class ElasticsearchProjectRule extends ConverterRule
{
    public ElasticsearchProjectRule()
    {
        super(LogicalProject.class, Convention.NONE, ElasticsearchRelNode.CONVENTION,
                ElasticsearchProjectRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode rel)
    {
        LogicalProject project = (LogicalProject) rel;
        List<RexNode> projects = project.getProjects();
        RelTraitSet traitSet = project.getTraitSet().replace(getOutTrait());

        return new ElasticsearchProject(project.getCluster(), traitSet,
                convert(project.getInput(), getOutTrait()), projects, project.getRowType());
    }

    public RelDataType getProjectFields(List<RexNode> rexNodes)
    {
        LogicalProject project = null;
        for(RexNode node : rexNodes)
        {
            if(node instanceof RexInputRef) {
                node.getType();
                RelDataTypeField typeField = project.getRowType().getFieldList().get(0);
                typeField.equals(null);
            }
        }
        return null;
    }
}
