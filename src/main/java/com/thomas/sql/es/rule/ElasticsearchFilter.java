package com.thomas.sql.es.rule;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.thomas.sql.es.ElasticsearchRelNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ElasticsearchFilter extends Filter implements ElasticsearchRelNode
{
    private final Multimap<String, Pair<SqlKind, RexLiteral>> multimap = HashMultimap.create();

    public ElasticsearchFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition)
    {
        super(cluster, traits, child, condition);
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition)
    {
        return new ElasticsearchFilter(getCluster(), traitSet, input, condition);
    }

    /**
     * decompose a condition by OR into a list of sub-condition
     * These sub-conditions might be "atomic" or able to be decomposed by AND
     */
    public QueryBuilder translate()
    {
        final List<RexNode> orNodes = RelOptUtil.disjunctions(condition);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        for (RexNode node : orNodes)
            boolQueryBuilder.should(translateAnd(node));
        return boolQueryBuilder;
    }

    private static Object literalValue(RexLiteral literal) {
        return literal.getValue2();
    }

    /**
     * 解析一个条件结点（可能是其他诸多子条件的“与”）
     * 合并针对同一个filed的多个条件
     */
    private QueryBuilder translateAnd(RexNode node0) {
        multimap.clear();
        for (RexNode node : RelOptUtil.conjunctions(node0))
            translateMatch(node);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        for (Map.Entry<String, Collection<Pair<SqlKind, RexLiteral>>> entry : multimap.asMap().entrySet())
        {
            for (Pair<SqlKind, RexLiteral> pair : entry.getValue())
            {
                String key = entry.getKey().toLowerCase();
                Object value = literalValue(pair.right);
                switch (pair.left) {
                    case EQUALS:
                        boolQueryBuilder.must(QueryBuilders.termQuery(key, value));
                        break;
                    case LESS_THAN:
                        boolQueryBuilder.must(QueryBuilders.rangeQuery(key).from(null).to(value, false));
                        break;
                    case LESS_THAN_OR_EQUAL:
                        boolQueryBuilder.must(QueryBuilders.rangeQuery(key).from(null).to(value, true));
                        break;
                    case NOT_EQUALS:
                        boolQueryBuilder.mustNot(QueryBuilders.termQuery(key, value));
                        break;
                    case GREATER_THAN:
                        boolQueryBuilder.must(QueryBuilders.rangeQuery(key).from(value, false).to(null));
                        break;
                    case GREATER_THAN_OR_EQUAL:
                        boolQueryBuilder.must(QueryBuilders.rangeQuery(key).from(value, true).to(null));
                        break;
                    case LIKE:
                        if(!(value instanceof String))
                            throw new IllegalArgumentException("invalid type of value " + value + ". String type is expected!");
                        String _value = (String) value;
                        StringBuilder stringBuilder = new StringBuilder(_value);
                        //wildcard replace. The _ in sql "equals to" ? in lucene
                        //The % in sql "equals to" * in lucene
                        Pattern pattern = Pattern.compile("(\\w_)|(\\w%)");
                        Matcher matcher = pattern.matcher(_value);
                        while (matcher.find())
                        {
                            String group = matcher.group();
                            if(group.endsWith("_"))
                                stringBuilder.replace(matcher.start(), matcher.end(), group.replace("_", "?"));
                            else if(group.endsWith("%"))
                                stringBuilder.replace(matcher.start(), matcher.end(), group.replace("%", "*"));
                        }

                        //SQL use \_ and \% to escape.
                        //but elasticsearch doesn't need it
                        pattern = Pattern.compile("(\\\\_)|(\\\\%)");
                        matcher = pattern.matcher(stringBuilder.toString());
                        while (matcher.find())
                        {
                            String group = matcher.group();
                            stringBuilder.replace(matcher.start(), matcher.end(), group.replace("\\", ""));
                        }
                        _value = stringBuilder.toString();
                        //optimize. using prefix query instead of wildcard query
                        if(_value.matches("^\\w+\\*$"))
                            boolQueryBuilder.must(QueryBuilders.prefixQuery(key, _value.substring(0, _value.indexOf("*"))));
                        else boolQueryBuilder.must(QueryBuilders.wildcardQuery(key, _value));
                        break;
                    case IS_NULL:
                        boolQueryBuilder.mustNot(QueryBuilders.existsQuery(key));
                        break;
                    case IS_NOT_NULL:
                        boolQueryBuilder.must(QueryBuilders.existsQuery(key));
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported operation of " + node0);
                }
            }
        }
        return boolQueryBuilder;
    }

    private Void translateMatch(RexNode node) {
        switch (node.getKind()) {
            case EQUALS:
            case NOT_EQUALS:
            case LIKE:
            case IS_NULL:
            case IS_NOT_NULL:
                return translateBinary(node.getKind(), node.getKind(), (RexCall) node);
            case LESS_THAN:
                return translateBinary(SqlKind.LESS_THAN, SqlKind.GREATER_THAN, (RexCall) node);
            case LESS_THAN_OR_EQUAL:
                return translateBinary(SqlKind.LESS_THAN_OR_EQUAL, SqlKind.GREATER_THAN_OR_EQUAL, (RexCall) node);
            case GREATER_THAN:
                return translateBinary(SqlKind.GREATER_THAN, SqlKind.LESS_THAN, (RexCall) node);
            case GREATER_THAN_OR_EQUAL:
                return translateBinary(SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.LESS_THAN_OR_EQUAL, (RexCall) node);
            default:
                throw new UnsupportedOperationException("cannot translate " + node);
        }
    }

    /**
     * binary operator translation.
     * Reverse the position of operands if necessary. For example, "10 <= age" to "age >= 10"
     * We expect the right operand to be literal
     */
    private Void translateBinary(SqlKind operator, SqlKind reverseOperator, RexCall call) {
        final RexNode left = call.operands.get(0);
        final RexNode right = call.operands.get(1);
        boolean res = translateBinary(operator, left, right);
        if (res) return null;
        res = translateBinary(reverseOperator, right, left);
        if (res) return null;
        throw new UnsupportedOperationException("cannot translate operator " + operator + " call " + call);
    }

    private boolean translateBinary(SqlKind operator, RexNode left, RexNode right) {
        if (right.getKind() != SqlKind.LITERAL) return false;
        final RexLiteral rightLiteral = (RexLiteral) right;
        switch (left.getKind())
        {
            case INPUT_REF:
                String name = getRowType().getFieldNames().get(((RexInputRef) left).getIndex());
                translateOp(operator, name, rightLiteral);
                return true;
            case CAST:
                return translateBinary(operator, ((RexCall) left).operands.get(0), right);
            default: return false;
        }
    }

    private void translateOp(SqlKind op, String name, RexLiteral right) {
        multimap.put(name, Pair.of(op, right));
    }


    @Override
    public void implement(Implementor implementor)
    {
        //always visit the child node (input) first
        implementor.visitChild(0, getInput());
        QueryBuilder queryBuilder = translate();
        implementor.getElasticsearchTable().addQueryBuilder(queryBuilder);
    }
}
