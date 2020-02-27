package com.oppo.tagbase.query;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.inject.Inject;
import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.meta.obj.Column;
import com.oppo.tagbase.meta.obj.ColumnType;
import com.oppo.tagbase.meta.obj.Table;
import com.oppo.tagbase.meta.type.DataType;
import com.oppo.tagbase.query.exception.SemanticException;
import com.oppo.tagbase.query.node.*;
import com.oppo.tagbase.query.row.RowMeta;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.oppo.tagbase.query.exception.SemanticErrorCode.*;

/**
 * @author huangfeng
 * @date 2020/2/9
 */
public class SemanticAnalyzer {

    private Metadata meta;
    private static final LocalDate LOW_BOUND = LocalDate.parse("2007-12-03");
    private static DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    //    private static DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    @Inject
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

            String dbName = analyzeDB(query);
            Table table = analyzeTable(query, dbName);

            // analysis for filter and groupby columns
            List<Filter> filters = query.getFilters();
            Map<String, FilterAnalysis> columnDomains = analyzeFilter(query, table, filters);

            // group columns equal  output dimension column
            List<String> groupByColumns = query.getDimensions();
            List<DataType> groupByColumnTypes = analyzeGroupBy(query, table);

            int groupMaxSize = evaluateGroupMaxSize(groupByColumns, columnDomains, table);
            int outputMaxSize = evaluateOutputSize(groupByColumns, columnDomains);

            RowMeta rowMeta = new RowMeta(nextSingleQueryId(), groupByColumns, groupByColumnTypes);
            Scope scope = Scope.builder().withOutputType(query.getOutput()).addRowMeta(rowMeta).withOutputSize(outputMaxSize).withGroupMaxSize(groupMaxSize).build();
            analysis.addScope(query, scope);
            return scope;
        }


        @Override
        public Scope visitComplexQuery(ComplexQuery query) {
            List<Query> subQueries = query.getSubQueries();

            //function support >2 subquerys, but it is not correct in semantic
            if (subQueries.size() != 2) {
                throw new SemanticException(NOT_SUPPORTED, "subQuery size must be 2");
            }

            Scope leftScope = subQueries.get(0).accept(this);
            if (leftScope.getOutputType() != OutputType.BITMAP) {
                throw new SemanticException(NOT_SUPPORTED, "complex query only work for bitmap");
            }


            Scope.Builder scopeBuilder = Scope.builder().withOutputType(query.getOutput());
            List<RowMeta> leftOutRelations = leftScope.getOutRelations();
            int count = leftScope.getOutPutSize();


            for (int n = 1; n < subQueries.size(); n++) {
                Scope scope = subQueries.get(n).accept(this);
                if (scope.getOutputType() != OutputType.BITMAP) {
                    throw new SemanticException(NOT_SUPPORTED, "complex query only work for bitmap");
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

        private String nextSingleQueryId() {
            singleQueryId++;
            return singleQueryId + "";
        }


        // max groupMaxsize for singleQuery equal the product of  column cardinality which not in
        private int evaluateGroupMaxSize(List<String> groupbyColumns, Map<String, FilterAnalysis> columnDomains, Table table) {
            int groupMaxSize = 1;
            Set<String> filterColumns = columnDomains.keySet();
            for (String columnName : filterColumns) {
                int cardinality = columnDomains.get(columnName).getCardinality();
                if (!groupbyColumns.contains(columnName)) {
                    if (cardinality == Integer.MAX_VALUE) {
                        groupMaxSize = Integer.MAX_VALUE;
                        break;
                    }
                    groupMaxSize *= cardinality;
                }
            }

            if (groupMaxSize != Integer.MAX_VALUE) {
                int dimensionColumnsSize = table.getColumns().size() - 1;
                if (ImmutableSet.<String>builder().addAll(groupbyColumns).addAll(filterColumns).build().size() != dimensionColumnsSize) {
                    groupMaxSize = Integer.MAX_VALUE;
                }
            }
            return groupMaxSize;
        }

        private List<DataType> analyzeGroupBy(SingleQuery query, Table table) {
            List<String> groupByColumns = query.getDimensions();
            Set<String> dimColumns = new HashSet<>();
            List<DataType> outputFields = new ArrayList<>();
            for (String dim : groupByColumns) {
                if (dimColumns.contains(dim)) {
                    throw new SemanticException(DUPLICATE_FILTER_COLUMN, "duplicate column %s in dimension", dim);
                }
                if (table.getColumn(dim) == null) {
                    throw new SemanticException(MISSING_COLUMN, "column %s doesn't exist in table %s ", dim, table.getName());
                }

                dimColumns.add(dim);
                outputFields.add(table.getColumn(dim).getDataType());
            }
            analysis.addGroupByColumns(query, groupByColumns);
            return outputFields;
        }


        private int evaluateOutputSize(List<String> groupbyColumns, Map<String, FilterAnalysis> columnDomains) {
            int outputMaxSize = 1;
            // max outputSize for singleQuery equal the product of groupby column cardinality
            for (String columnName : groupbyColumns) {
                FilterAnalysis columnAnalysis = columnDomains.get(columnName);
                int cardinality = columnAnalysis == null ? Integer.MAX_VALUE : columnAnalysis.getCardinality();
                if (cardinality == Integer.MAX_VALUE) {
                    outputMaxSize = Integer.MAX_VALUE;
                    break;
                } else {
                    outputMaxSize *= cardinality;
                }
            }
            return outputMaxSize;
        }

        private Map<String, FilterAnalysis> analyzeFilter(SingleQuery query, Table table, List<Filter> filters) {
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


                    BoundFilter boundFilter = (BoundFilter) filter;
                    FilterAnalysis sliceDomain;
                    int gapDay;
                    try {
                        LocalDate lower = parseDate(boundFilter.getLower(), LOW_BOUND);
                        LocalDate upper = parseDate(boundFilter.getUpper(), LocalDate.now());

                        gapDay = getDayInterval(lower, upper, boundFilter.isUpperStrict(), boundFilter.isLowerStrict());

                        LocalDateTime lowerTime = boundFilter.isLowerStrict() ? lower.plusDays(1).atStartOfDay() : lower.atStartOfDay();
                        LocalDateTime upperTime = boundFilter.isUpperStrict() ? upper.atStartOfDay() : upper.plusDays(1).atStartOfDay();
                        BoundType lowBoundType = BoundType.CLOSED;
                        BoundType upperBoundType = BoundType.OPEN;

                        sliceDomain = new FilterAnalysis(column, ImmutableRangeSet.of(Range.range(lowerTime, lowBoundType, upperTime, upperBoundType)), gapDay);


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
            analysis.addFilterAnalysis(query, filterAnalysisMap);
            return filterAnalysisMap;

        }


        private int getDayInterval(LocalDate lower, LocalDate upper, boolean upperStrict, boolean lowerStrict) {
            int days = Period.between(lower, upper).getDays();
            days = lowerStrict ? --days : days;
            days = upperStrict ? days : ++days;
            return days;
        }


        private LocalDate parseDate(String dateStr, LocalDate defaultValue) throws ParseException {
            return dateStr == null ? defaultValue : LocalDate.parse(dateStr, DATE_TIME_FORMATTER);
        }

        private Table analyzeTable(SingleQuery query, String dbName) {
            Table table = meta.getTable(dbName, query.getTableName());
            if (table == null) {
                throw new SemanticException(MISSING_TABLE, "table %s doesn't exist", dbName);
            }
            analysis.addTable(query, table);
            return table;
        }


        private String analyzeDB(SingleQuery query) {
            String dbName = query.getDbName();
            if (meta.getDb(dbName) == null) {
                throw new SemanticException(MISSING_DB, "db %s doesn't exist", dbName);
            }
            analysis.addDB(query, dbName);
            return dbName;
        }


    }


}
