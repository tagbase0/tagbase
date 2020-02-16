package com.oppo.tagbase.query;

import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.meta.obj.Table;
import com.oppo.tagbase.query.node.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author huangfeng
 * @date 2020/2/9
 */
public class SemanticAnalyzer {

    private Metadata meta;

    public SemanticAnalyzer(Metadata meta) {
        this.meta = meta;
    }

    public void analyze(Query query) {
        query.accept(new Visitor<Analysis>(meta));
    }


    private static class Visitor<Analysis> implements QueryVisitor<SemanticAnalyzer.Analysis> {
        Metadata meta;

        Visitor(Metadata meta) {
            this.meta = meta;
        }

        @Override
        public SemanticAnalyzer.Analysis visitSingleQuery(SingleQuery query) {
            //1. 对于查询的表， 指定的列需要真实存在
            // 2. 单表需要指定查询时间段

            //validate whether table exist
            Table table = meta.getTable(null, query.getTableName());

            int columnSize = 0;

            Set<String> filterColumns = new HashSet<>();
            boolean isAllExact = true;
            for (Filter filter : query.getFilters()) {



                String columnName = filter.getColumn();
                if(!table.getColumns().contains(columnName)){
                    //  column doesn't exist in table

                }

                if (filterColumns.contains(columnName)) {
                    // multiple filters for same column
                }

                if (filter instanceof InFilter) {
                    // value size must >= 1
                }


                if (!filter.isExact()) {
                    isAllExact = false;
                }

            }

            if (isAllExact && filterColumns.size() == columnSize) {
                return new SemanticAnalyzer.Analysis(1, query.getOutput());
            }
            return new SemanticAnalyzer.Analysis(query.getOutput());
        }

        @Override
        public SemanticAnalyzer.Analysis visitComplexQuery(ComplexQuery query) {
            // 3. 对于complexQuery 分析输出的长度 对于第一个childQuery只能输出一个（全限定了就是输出1一个），


            List<Query> subQueries = query.getSubQueries();
            if (subQueries.size() <= 1) {

            }
            int count = 0;
            for (int n = 0; n < subQueries.size(); n++) {
                SemanticAnalyzer.Analysis analysis = subQueries.get(n).accept(this);
                if (analysis.getOutputType() != OutputType.BITMAP) {
                    // complex input must bitmap
                }
                if (n == 0 && analysis.getOutPutSize() != 1) {
                    // fist subquery output size must be 1
                }


                if(n >= 1){
                    count +=  analysis.getOutPutSize();
                }


            }

            if(subQueries.size() == 2 && count == 1){
                return new SemanticAnalyzer.Analysis(1, query.getOutputType());
            }

            return new SemanticAnalyzer.Analysis(count, query.getOutputType());
        }
    }


    private static class Analysis {
        int outPutSize;
        OutputType outputType;

        Analysis(int outPutSize, OutputType outputType) {
            this.outPutSize = outPutSize;
            this.outputType = outputType;
        }

        Analysis(OutputType output) {
            this.outputType = output;
            outPutSize = Integer.MAX_VALUE;
        }

        public int getOutPutSize() {
            return outPutSize;
        }

        public OutputType getOutputType() {
            return outputType;
        }
    }

}
