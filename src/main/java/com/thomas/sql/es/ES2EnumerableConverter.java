package com.thomas.sql.es;

import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.BuiltInMethod;

import java.util.List;

public class ES2EnumerableConverter extends ConverterImpl implements EnumerableRel
{
    public ES2EnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode child)
    {
        super(cluster, ConventionTraitDef.INSTANCE, traits, child);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs)
    {
        return new ES2EnumerableConverter(getCluster(), traitSet, inputs.get(0));
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref)
    {
        final ElasticsearchRelNode.Implementor esImplementor =
                new ElasticsearchRelNode.Implementor();
        esImplementor.visitChild(0, getInput());
        final RelDataType rowType = getRowType();
        final PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), rowType,
                pref.prefer(JavaRowFormat.ARRAY));

        ElasticsearchTable esTable = esImplementor.elasticsearchTable;
        List<Object[]> resultList = esTable.search();
        return implementor.result(physType, Blocks.toBlock(Expressions.call(BuiltInMethod.AS_ENUMERABLE2.method, Expressions.constant(resultList.toArray()))));
    }
}
