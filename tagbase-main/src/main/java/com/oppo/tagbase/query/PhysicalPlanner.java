package com.oppo.tagbase.query;

import com.oppo.tagbase.query.node.ComplexQuery;
import com.oppo.tagbase.query.node.Query;
import com.oppo.tagbase.query.node.QueryVisitor;
import com.oppo.tagbase.query.node.SingleQuery;
import com.oppo.tagbase.query.operator.Operator;
import com.oppo.tagbase.query.operator.OperatorBuffer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author huangfeng
 * @date 2020/2/9
 */
public class PhysicalPlanner {


    public List<Operator> plan(Query query){
        List<Operator> operators = new ArrayList<>();
        new Visitor().process(query,operators);
        return operators;
    }

    private static class Visitor implements QueryVisitor<Void>{
        List<Operator> operators;
        private final LinkedList<OperatorBuffer> stack = new LinkedList<>();

        public void process(Query query,List<Operator> operators){
            this.operators = operators;
            query.accept(this);
        }

        public void pushContext(OperatorBuffer buffer){
            stack.push(buffer);
        }

        public void popContext(){
            stack.pop();
        }

        public OperatorBuffer getContext(){
            return stack.peek();
        }


        @Override
        public Void visitSingleQuery(SingleQuery query) {

            OperatorBuffer outPutBuffer = getContext();




            return null;

        }

        @Override
        public Void visitComplexQuery(ComplexQuery query) {
            OperatorBuffer outPutBuffer = getContext();
            List<Query> subQueries = query.getSubQueries();

            Query leftQuery =  subQueries.get(0);
            OperatorBuffer left = new OperatorBuffer();

            pushContext(left);
            leftQuery.accept(this);
            popContext();


            OperatorBuffer others = new OperatorBuffer();
            pushContext(others);

            for(int n=1;n<subQueries.size();n++){
                subQueries.get(n).accept(this);
            }





            return null;
        }


    }

}
