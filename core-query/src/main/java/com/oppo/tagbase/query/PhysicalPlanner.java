package com.oppo.tagbase.query;

import com.oppo.tagbase.meta.obj.ColumnType;
import com.oppo.tagbase.meta.obj.Table;
import com.oppo.tagbase.query.node.*;
import com.oppo.tagbase.query.operator.*;
import com.oppo.tagbase.storage.core.connector.StorageConnector;
import com.oppo.tagbase.storage.core.obj.ColumnDomain;
import com.oppo.tagbase.storage.core.obj.QueryHandler;

import java.sql.Date;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author huangfeng
 * @date 2020/2/9
 */
public class PhysicalPlanner {


    public List<Operator> plan(Query query, Analysis analysis) {
        List<Operator> operators = new ArrayList<>();

        OperatorBuffer inputBuffer = new OperatorBuffer<>();
        new Visitor(analysis).process(query, operators, inputBuffer);

        Map<String, RowMeta> outputMeta = analysis.getScope(query).getOutPutMeta();

        if (query.getOutput() != OutputType.COUNT) {
            Operator persistResultOperator = new PersistResultOperator(inputBuffer);
            operators.add(persistResultOperator);
        } else {
            Operator outputOperator = new OutputOperator(inputBuffer, outputMeta);
            operators.add(outputOperator);
        }

        return operators;
    }

    private static class Visitor implements QueryVisitor<Void> {
        List<Operator> operators;
        Analysis analysis;
        private final LinkedList<OperatorBuffer> stack = new LinkedList<>();

         Visitor(Analysis analysis) {
            this.analysis = analysis;
        }

        public void process(Query query, List<Operator> operators, OperatorBuffer root) {
            this.operators = operators;
            stack.push(root);
            query.accept(this);
        }

        public void pushContext(OperatorBuffer buffer) {
            stack.push(buffer);
        }

        public void popContext() {
            stack.pop();
        }

        public OperatorBuffer getContext() {
            return stack.peek();
        }


        @Override
        public Void visitSingleQuery(SingleQuery query) {


            Scope scope = analysis.getScope(query);

            OperatorBuffer outputBuffer = getContext();


            StorageConnector connector = null;

            String dbName = analysis.getQueryDB(query);
            Table table = analysis.getQueryTable(query);
            List<String> dims = analysis.getDims(query);

            Map<String, FilterAnalysis> filterAnalysisMap = analysis.getQueryFilterAnalysis(query);

            List<ColumnDomain<String>> dimColumnList = new ArrayList<>();
            ColumnDomain<Date> sliceColumn = null;

            for (FilterAnalysis filterAnalysis : filterAnalysisMap.values()) {
                if (filterAnalysis.getColumn().getType() == ColumnType.SLICE_COLUMN) {
                    sliceColumn = new ColumnDomain<>(filterAnalysis.getColumnRange(), filterAnalysis.getColumn().getName());
                } else {
                    dimColumnList.add(new ColumnDomain<>(filterAnalysis.getColumnRange(), filterAnalysis.getColumn().getName()));
                }
            }

            QueryHandler queryHandler = new QueryHandler(dbName, table.getName(), dims, dimColumnList, sliceColumn);
            int maxGroupSize = scope.getGroupMaxSize();
            operators.add(new SingleQueryOperator(queryHandler, outputBuffer, connector, maxGroupSize, scope.getOutRelations().get(0).getID()));
            return null;

        }

        @Override
        public Void visitComplexQuery(ComplexQuery query) {
            OperatorBuffer outPutBuffer = getContext();


            List<Query> subQueries = query.getSubQueries();

            Query leftQuery = subQueries.get(0);
            OperatorBuffer left = new OperatorBuffer();
            pushContext(left);
            leftQuery.accept(this);


            popContext();
            OperatorBuffer others = new OperatorBuffer();
            pushContext(others);
            for (int n = 1; n < subQueries.size(); n++) {
                subQueries.get(n).accept(this);
            }

            operators.add(new CollectionOperator(left, others, outPutBuffer, query.getOperation(),query.getOutputType()));

            return null;
        }


    }

}
