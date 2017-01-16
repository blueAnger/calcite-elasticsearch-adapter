package com.thomas.sql.es;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.TranslatableTable;
import org.elasticsearch.client.transport.TransportClient;

public class ElasticsearchTranslatableTable extends ElasticsearchTable implements TranslatableTable
{
    public ElasticsearchTranslatableTable(TransportClient client, String index, String type)
    {
        super(client, index, type);
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable)
    {
        return new ElasticsearchTableScan(context.getCluster(), relOptTable, this);
    }
}
