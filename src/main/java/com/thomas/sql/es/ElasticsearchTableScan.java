package com.thomas.sql.es;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableScan;

import java.util.List;

public class ElasticsearchTableScan extends TableScan implements ElasticsearchRelNode
{
    private ElasticsearchTable esTable;
    private List<? extends RelOptRule> rules;

    public ElasticsearchTableScan(RelOptCluster cluster, RelOptTable table, ElasticsearchTable esTable)
    {
        super(cluster, cluster.traitSetOf(ElasticsearchRelNode.CONVENTION), table);
        this.esTable = esTable;
    }

    @Override
    public void register(RelOptPlanner planner)
    {
        //add the specific rule to do something about elasticsearch to optimize
        planner.addRule(new ES2EnumerableConverterRule());
        rules = ElasticsearchRules.createRules("filter", "aggregate");
        for(RelOptRule rule : rules)
            planner.addRule(rule);
    }

    @Override
    public void implement(Implementor implementor)
    {
        implementor.table = table;
        implementor.elasticsearchTable = esTable;
    }
}
