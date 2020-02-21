package com.oppo.tagbase.query;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.meta.obj.Column;
import com.oppo.tagbase.meta.obj.ColumnType;
import com.oppo.tagbase.meta.obj.Table;
import com.oppo.tagbase.meta.type.DataType;
import com.oppo.tagbase.query.exception.SemanticException;
import com.oppo.tagbase.query.node.*;
import com.oppo.tagbase.query.operator.RowMeta;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static com.oppo.tagbase.query.exception.SemanticErrorCode.*;

/**
 * @author huangfeng
 * @date 2020/2/9
 */
public class SemanticAnalyzer {

    private Metadata meta;
    private static final Date LOW_BOUND = new Date(1577808000);


    //    private static DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    public SemanticAnalyzer(Metadata meta) {
        this.meta = meta;
    }

    public Analysis analyze(Query query) {
        Analysis analysis = new Analysis();

        new Visitor(meta, analysis).process(query);

        return analysis;
    }


    private static class Visitor implements QueryVisitor<Scope> {
        int singleQueryId;
        Metadata meta;
        Analysis analysis;

        Visitor(Metadata meta, Analysis analysis) {
            singleQueryId = 0;
            this.meta = meta;
            this.analysis = analysis;
        }

        public Scope process(Query query) {
            return query.accept(this);
        }


        @Override
        public Scope visitSingleQuery(SingleQuery query) {
            singleQueryId++;

            String dbName = analyzeDB(query.getDbName());
            Table table = analyzeTable(dbName, query.getTableName());

            List<Filter> filters = query.getFilters();
            Map<String, FilterAnalysis> columnDomains = analyzeFilter(table, filters);
            List<String> filterColumns = filters.stream().map(filter -> filter.getColumn()).collect(Collectors.toList());


            List<String> dimColumns = query.getDimensions();
            List<DataType> outputFields = analyzeGroupBy(table, dimColumns);

            // evaluate outputSize for singleQuery
            int outputMaxSize = 1;
            for (String columnName : dimColumns) {
                FilterAnalysis columnAnalysis = columnDomains.get(columnName);
                int valueCount = columnAnalysis == null ? Integer.MAX_VALUE : columnAnalysis.getCardinality();
                if (valueCount == Integer.MAX_VALUE) {
                    outputMaxSize = Integer.MAX_VALUE;
                    break;
                } else {
                    outputMaxSize *= valueCount;
                }
            }

            // evaluate group data Maxcount for SingleQuery
            int groupMaxSize = 1;
            for (String columnName : filterColumns) {
                int valueCount = columnDomains.get(columnName).getCardinality();
                if (!dimColumns.contains(columnName)) {
                    if (valueCount == Integer.MAX_VALUE) {
                        groupMaxSize = Integer.MAX_VALUE;
                        break;
                    }
                    groupMaxSize *= valueCount;
                }
            }

            if (groupMaxSize != Integer.MAX_VALUE) {
                if (ImmutableSet.<String>builder().addAll(dimColumns).addAll(filterColumns).build().size() != table.getColumns().size()) {
                    groupMaxSize = Integer.MAX_VALUE;
                }
            }

            RowMeta rowMeta = new RowMeta(dimColumns, outputFields, singleQueryId + "");
            Scope scope = Scope.builder().withOutputType(query.getOutput()).addRowMeta(rowMeta).withOutputSize(outputMaxSize).withroupMaxSize(groupMaxSize).build();
            analysis.addScope(query, scope);
            return scope;
        }

        private List<DataType> analyzeGroupBy(Table table, List<String> dims) {
            Set<String> dimColumns = new HashSet<>();
            List<DataType> outputFields = new ArrayList<>();
            for (String dim : dims) {
                if (dimColumns.contains(dim)) {
                    throw new SemanticException(DUPLICATE_FILTER_COLUMN, "duplicate column %s in dimension", dim);
                }
                if (table.getColumn(dim) == null) {
                    throw new SemanticException(MISSING_COLUMN, "column %s doesn't exist in table %s ", dim, table.getName());
                }

                dimColumns.add(dim);
                outputFields.add(table.getColumn(dim).getDataType());
            }
            return outputFields;
        }

        private Map<String, FilterAnalysis> analyzeFilter(Table table, List<Filter> filters) {
            Set<String> filterColumnSet = new HashSet<>();

            Map<String, FilterAnalysis> filterAnalysisMap = new HashMap<>();


            for (Filter filter : filters) {
                String columnName = filter.getColumn();
                if (filterColumnSet.contains(columnName)) {
                    throw new SemanticException(DUPLICATE_FILTER_COLUMN, "duplicate column %s in filter", columnName);
                }
                filterColumnSet.add(columnName);
                Column column = table.getColumn(columnName);
                if (column == null) {
                    throw new SemanticException(MISSING_COLUMN, "column %s doesn't exist in table %s ", columnName, table.getName());
                }

                if (column.getType() == ColumnType.SLICE_COLUMN) {
                    if (filter instanceof InFilter) {
                        throw new SemanticException(SLICE_MUST_BE_BOUND_FILTER, "slice column must be bound filter", columnName);
                    }

                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
                    BoundFilter boundFilter = (BoundFilter) filter;
                    FilterAnalysis sliceDomain;
                    int gapDay;
                    try {
                        Date lower = parseDate(dateFormat, boundFilter.getLower(), LOW_BOUND);
                        Date upper = parseDate(dateFormat, boundFilter.getUpper(), new Date());
                        BoundType upperBoundType = boundFilter.isUpperStrict() ? BoundType.CLOSED : BoundType.OPEN;
                        BoundType lowBoundType = boundFilter.isLowerStrict() ? BoundType.CLOSED : BoundType.OPEN;

                        gapDay = getDayInterval(lower, upper, boundFilter.isUpperStrict(), boundFilter.isLowerStrict());
                        sliceDomain = new FilterAnalysis(column, ImmutableRangeSet.of(Range.range(lower, lowBoundType, upper, upperBoundType)), gapDay);
                    } catch (ParseException e) {
                        throw new SemanticException(WRONG_DATE_FORMAT, "wrong date format");
                    }

                    filterAnalysisMap.put(columnName, sliceDomain);


                } else if (column.getType() == ColumnType.DIM_COLUMN) {
                    if (column.getDataType() != DataType.STRING) {
                        throw new SemanticException(NOT_SUPPORTED, "column %s must be string", column.getName());
                    }

                    filterAnalysisMap.put(columnName, new FilterAnalysis(column, filter.getDimensionRangeSet(), ((InFilter) filter).getValues().size()));

                } else {
                    throw new SemanticException(NOT_SUPPORTED, "metric column doesn't support filter");
                }
            }
            return filterAnalysisMap;

        }

        private int getDayInterval(Date lower, Date upper, boolean upperStrict, boolean lowerStrict) {
            return 1;
        }

        private Date parseDate(SimpleDateFormat dateFormat, String dateStr, Date defaultValue) throws ParseException {
            if (dateStr == null) {
                return defaultValue;
            } else {
                return dateFormat.parse(dateStr);
            }
        }

        private Table analyzeTable(String dbName, String tableName) {
            Table table = meta.getTable(dbName, tableName);
            analysis.addTable(table);
            if (table == null) {
                throw new SemanticException(MISSING_TABLE, "table %s doesn't exist", dbName);
            }
            return table;
        }


        private String analyzeDB(String dbName) {
            if (meta.getDb(dbName) == null) {
                throw new SemanticException(MISSING_DB, "db %s doesn't exist", dbName);
            }
            return dbName;
        }



        @Override
        public Scope visitComplexQuery(ComplexQuery query) {
            List<Query> subQueries = query.getSubQueries();
            if (subQueries.size() != 2) {
                throw new SemanticException(NOT_SUPPORTED,"subQuery size must be 2");
            }

            Scope leftScope = subQueries.get(0).accept(this);
            if (leftScope.getOutputType() != OutputType.BITMAP) {
                // complex input must bitmap
                throw  new SemanticException(NOT_SUPPORTED,"complex query only work for bitmap");
            }


            Scope.Builder scopeBuilder = Scope.builder().withOutputType(query.getOutput());
            List<RowMeta> leftOutRelations = leftScope.getOutRelations();
            int count = leftScope.getOutPutSize();


            for (int n = 1; n < subQueries.size(); n++) {
                Scope scope = subQueries.get(n).accept(this);
                if (scope.getOutputType() != OutputType.BITMAP) {
                    throw new SemanticException(NOT_SUPPORTED,"complex query only work for bitmap");
                }

                if (count != Integer.MAX_VALUE && scope.getOutPutSize() != Integer.MAX_VALUE) {
                    count += leftScope.getOutPutSize() * scope.getOutPutSize();
                }

                List<RowMeta> rightOutRelations = scope.getOutRelations();

                for (RowMeta leftRowMeta : leftOutRelations) {
                    for (RowMeta rightRowMeta : rightOutRelations) {
                        scopeBuilder.addRowMeta(RowMeta.join(leftRowMeta, rightRowMeta));
                    }
                }
            }

            scopeBuilder.withOutputSize(count);
            Scope scope = scopeBuilder.build();
            analysis.addScope(query, scope);
            return scope;

        }


    }


}
