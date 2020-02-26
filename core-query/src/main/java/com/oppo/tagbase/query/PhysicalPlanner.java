package com.oppo.tagbase.query;

import com.oppo.tagbase.meta.obj.ColumnType;
import com.oppo.tagbase.meta.obj.Table;
import com.oppo.tagbase.query.node.*;
import com.oppo.tagbase.query.operator.*;
import com.oppo.tagbase.query.row.RowMeta;
import com.oppo.tagbase.storage.core.connector.StorageConnector;
import com.oppo.tagbase.storage.core.obj.ColumnDomain;
import com.oppo.tagbase.storage.core.obj.QueryHandler;
import org.javatuples.Pair;

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


    StorageConnector connector = null;

    private static final int ROOT_ID = 0;


    public PhysicalPlan plan(Query query, Analysis analysis) {


        PhysicalPlan.PhysicalPlanBuilder planBuilder = PhysicalPlan.builder();
        planBuilder.setQueryId(query.getId());

        OperatorBuffer inputBuffer = new OperatorBuffer<>();
         new Visitor(analysis, query).process(planBuilder,inputBuffer);


        Operator outputOperator;
        Map<String, RowMeta> outputMeta = analysis.getScope(query).getOutputMeta();
        if (query.getOutput() != OutputType.COUNT) {
            outputOperator = new PersistResultOperator(ROOT_ID, inputBuffer);
        } else {
            outputOperator = new OutputOperator(ROOT_ID, inputBuffer, outputMeta);
        }
        planBuilder.addOperator(-1,outputOperator);

        return  planBuilder.build();
    }


    private class Visitor implements QueryVisitor<Void> {


        private final LinkedList<Pair<Integer, OperatorBuffer>> stack = new LinkedList<>();


        Analysis analysis;

        Query rootQuery;
        int incrementId = 1;

        PhysicalPlan.PhysicalPlanBuilder planBuilder;

        Visitor(Analysis analysis, Query query) {
            this.analysis = analysis;
            rootQuery = query;
        }

        public PhysicalPlan.PhysicalPlanBuilder process(PhysicalPlan.PhysicalPlanBuilder planBuilder,OperatorBuffer root) {
            this.planBuilder = planBuilder;
            stack.push(new Pair<>(0, root));
            return planBuilder;
        }

        public void pushContext(Pair<Integer, OperatorBuffer> buffer) {
            stack.push(buffer);
        }

        public void popContext() {
            stack.pop();
        }

        public OperatorBuffer getContextParentInputBuffer() {
            return stack.peek().getValue1();
        }

        public int getContextParentId() {
            return stack.peek().getValue0();
        }

        @Override
        public Void visitSingleQuery(SingleQuery query) {

            Scope scope = analysis.getScope(query);

            OperatorBuffer outputBuffer = getContextParentInputBuffer();
            int parentId = getContextParentId();


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

            QueryHandler queryHandler = new QueryHandler(dbName, table.getName(), dims, dimColumnList, sliceColumn,query.getId());
            int maxGroupSize = scope.getGroupMaxSize();

            planBuilder.addOperator(parentId, new SingleQueryOperator(incrementId++, queryHandler, outputBuffer, connector, maxGroupSize, scope.getOutRelations().get(0).getID()));

            return null;

        }

        @Override
        public Void visitComplexQuery(ComplexQuery query) {
            OperatorBuffer outputBuffer = getContextParentInputBuffer();
            int parentId = getContextParentId();

            int currentId = incrementId++;
            List<Query> subQueries = query.getSubQueries();

            Query leftQuery = subQueries.get(0);
            OperatorBuffer left = new OperatorBuffer();
            pushContext(Pair.with(currentId, left));
            leftQuery.accept(this);


            popContext();
            OperatorBuffer others = new OperatorBuffer();
            pushContext(Pair.with(currentId, others));
            for (int n = 1; n < subQueries.size(); n++) {
                subQueries.get(n).accept(this);
            }

            planBuilder.addOperator(parentId, new CollectionOperator(parentId, left, others, outputBuffer, query.getOperation(), query.getOutputType()));

            return null;
        }


    }

}
