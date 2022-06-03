/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.queries;

import java.util.Iterator;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationGroupByOrderByOperator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.DistinctOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


@SuppressWarnings("ConstantConditions")
public class InnerSegmentAggregationMultiValueQueriesTest extends BaseMultiValueQueriesTest {
  private static final String AGGREGATION_QUERY =
      "SELECT COUNT(*), SUM(column1), MAX(column2), MIN(column8), AVG(column9) FROM testTable";
  private static final String MULTI_VALUE_AGGREGATION_QUERY =
      "SELECT COUNTMV(column6), SUMMV(column7), MAXMV(column6), MINMV(column7), AVGMV(column6) FROM testTable";

  // ARRAY_BASED
  private static final String SMALL_GROUP_BY = " GROUP BY column7";
  // INT_MAP_BASED
  private static final String MEDIUM_GROUP_BY = " GROUP BY column3, column6, column7";
  // LONG_MAP_BASED
  private static final String LARGE_GROUP_BY = " GROUP BY column1, column3, column6, column7";
  // ARRAY_MAP_BASED
  private static final String VERY_LARGE_GROUP_BY =
      " GROUP BY column1, column2, column6, column7, column8, column9, column10";

  @Test
  public void testAggregationOnly() {
    // Test query without filter.
    AggregationOperator aggregationOperator = getOperator(AGGREGATION_QUERY);
    IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 100000L, 0L,
        400000L, 100000L);
    QueriesTestUtils.testInnerSegmentAggregationResult(resultsBlock.getAggregationResult(), 100000L, 100991525475000L,
        2147434110, 1182655, 83439903673981L, 100000L);

    // Test query with filter.
    aggregationOperator = getOperator(AGGREGATION_QUERY + FILTER);
    resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 15620L, 275416,
        62480L, 100000L);
    QueriesTestUtils.testInnerSegmentAggregationResult(resultsBlock.getAggregationResult(), 15620L, 17287754700747L,
        999943053, 1182655, 11017594448983L, 15620L);
  }

  @Test
  public void testMultiValueAggregationOnly() {
    // Test query without filter.
    AggregationOperator aggregationOperator = getOperator(MULTI_VALUE_AGGREGATION_QUERY);
    IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 100000L, 0L,
        200000L, 100000L);
    QueriesTestUtils.testInnerSegmentAggregationResult(resultsBlock.getAggregationResult(), 106688L, 107243218420671L,
        2147483647, 201, 121081150452570L, 106688L);

    // Test query with filter.
    aggregationOperator = getOperator(MULTI_VALUE_AGGREGATION_QUERY + FILTER);
    resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 15620L, 275416L,
        31240L, 100000L);
    QueriesTestUtils.testInnerSegmentAggregationResult(resultsBlock.getAggregationResult(), 15620L, 28567975886777L,
        2147483647, 203, 28663153397978L, 15620L);
  }

  @Test
  public void testSmallAggregationGroupBy() {
    // Test query without filter.
    AggregationGroupByOrderByOperator groupByOperator = getOperator(AGGREGATION_QUERY + SMALL_GROUP_BY);
    IntermediateResultsBlock resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), 100000L, 0L, 500000L,
        100000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        new Object[]{201}, 26L, 32555949195L, 2100941020, 117939666, 23061775005L, 26L);

    // Test query with filter.
    groupByOperator = getOperator(AGGREGATION_QUERY + FILTER + SMALL_GROUP_BY);
    resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), 15620L, 275416L,
        78100L, 100000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        new Object[]{203}, 1L, 185436225L, 987549258, 674022574, 674022574L, 1L);
  }

  @Test
  public void testMediumAggregationGroupBy() {
    // Test query without filter.
    AggregationGroupByOrderByOperator groupByOperator = getOperator(AGGREGATION_QUERY + MEDIUM_GROUP_BY);
    IntermediateResultsBlock resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), 100000L, 0L, 700000L,
        100000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        new Object[]{"w", 3818, 369}, 1L, 1095214422L, 1547156787, 528554902, 52058876L, 1L);

    // Test query with filter.
    groupByOperator = getOperator(AGGREGATION_QUERY + FILTER + MEDIUM_GROUP_BY);
    resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), 15620L, 275416L,
        109340L, 100000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        new Object[]{"L", 306633, 2147483647}, 1L, 131154783L, 952002176, 674022574, 674022574L, 1L);
  }

  @Test
  public void testLargeAggregationGroupBy() {
    // Test query without filter.
    AggregationGroupByOrderByOperator groupByOperator = getOperator(AGGREGATION_QUERY + LARGE_GROUP_BY);
    IntermediateResultsBlock resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), 100000L, 0L, 700000L,
        100000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        new Object[]{240129976, "L", 2147483647, 2147483647}, 1L, 240129976L, 1649812746, 2077178039, 1952924139L, 1L);

    // Test query with filter.
    groupByOperator = getOperator(AGGREGATION_QUERY + FILTER + LARGE_GROUP_BY);
    resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), 15620L, 275416L,
        109340L, 100000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        new Object[]{903461357, "L", 2147483647, 388}, 2L, 1806922714L, 652024397, 674022574, 870535054L, 2L);
  }

  @Test
  public void testVeryLargeAggregationGroupBy() {
    // Test query without filter.
    AggregationGroupByOrderByOperator groupByOperator = getOperator(AGGREGATION_QUERY + VERY_LARGE_GROUP_BY);
    IntermediateResultsBlock resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), 100000L, 0L, 700000L,
        100000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        new Object[]{1118965780, 1848116124, 8599, 504, 1597666851, 675163196, 607034543}, 1L, 1118965780L, 1848116124,
        1597666851, 675163196L, 1L);

    // Test query with filter.
    groupByOperator = getOperator(AGGREGATION_QUERY + FILTER + VERY_LARGE_GROUP_BY);
    resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), 15620L, 275416L,
        109340L, 100000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        new Object[]{949960647, 238753654, 2147483647, 2147483647, 674022574, 674022574, 674022574}, 2L, 1899921294L,
        238753654, 674022574, 1348045148L, 2L);
  }

  /**
   * Test DISTINCT on single column single segment. Since the dataset
   * is Avro files, the only thing we currently check
   * for correctness is the actual number of DISTINCT
   * records returned
   */
  @Test
  public void testSingleColumnDistinct() {

    // Firstly test groupby with aggregation
    AggregationGroupByOrderByOperator groupByOperator =
     getOperator("SELECT column7, count(*) FROM testTable group by column7 LIMIT 1000000");
    IntermediateResultsBlock gbResultsBlock = groupByOperator.nextBlock();
    AggregationGroupByResult agbResult = gbResultsBlock.getAggregationGroupByResult();
    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = agbResult.getGroupKeyIterator();
    int numGroups = 0;
    while (groupKeyIterator.hasNext()) {
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      numGroups++;
    }

    assertEquals(numGroups, 359);

    // Test distinct with the same column
    String query = "SELECT column7 FROM testTable where column5 != 3 group by column7 LIMIT 1000000";
    BaseOperator<IntermediateResultsBlock> distinctOperator = getOperator(query);
    IntermediateResultsBlock resultsBlock = distinctOperator.nextBlock();
    List<Object> operatorResult = resultsBlock.getAggregationResult();

    assertEquals(operatorResult.size(), 1);
    assertTrue(operatorResult.get(0) instanceof DistinctTable);

    DistinctTable distinctTable = (DistinctTable) operatorResult.get(0);
    assertEquals(distinctTable.size(), 359);

    DataSchema dataSchema = distinctTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"column7"});
    assertEquals(dataSchema.getColumnDataTypes(), new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});

    for (Record record : distinctTable.getRecords()) {
      assertNotNull(record);
      assertEquals(record.getValues().length, 1);
    }
  }

  /**
   * Test DISTINCT on multiple column single segment. Since the dataset
   * is Avro files, the only thing we currently check
   * for correctness is the actual number of DISTINCT
   * records returned
   */
  @Test
  public void testMultiColumnDistinct() {
    String query = "SELECT DISTINCT column1, column3 FROM testTable LIMIT 1000000";
    DistinctOperator distinctOperator = getOperator(query);
    IntermediateResultsBlock resultsBlock = distinctOperator.nextBlock();
    List<Object> operatorResult = resultsBlock.getAggregationResult();

    assertEquals(operatorResult.size(), 1);
    assertTrue(operatorResult.get(0) instanceof DistinctTable);

    DistinctTable distinctTable = (DistinctTable) operatorResult.get(0);
    assertEquals(distinctTable.size(), 21968);

    DataSchema dataSchema = distinctTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"column1", "column3"});
    assertEquals(dataSchema.getColumnDataTypes(),
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});

    for (Record record : distinctTable.getRecords()) {
      assertNotNull(record);
      assertEquals(record.getValues().length, 2);
    }
  }
}
