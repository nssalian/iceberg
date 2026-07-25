/*
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
package org.apache.iceberg.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Locale;
import org.apache.iceberg.parquet.VariantShreddingAnalyzer.InferenceStrategy;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;

public class TestInferenceStrategies {

  private static class StrategyAnalyzer extends VariantShreddingAnalyzer<VariantValue, Void> {
    StrategyAnalyzer(InferenceStrategy strategy) {
      super(strategy);
    }

    @Override
    protected List<VariantValue> extractVariantValues(List<VariantValue> rows, int idx) {
      return rows;
    }

    @Override
    protected int resolveColumnIndex(Void engineSchema, String columnName) {
      throw new UnsupportedOperationException("Not used in direct tests");
    }
  }

  // 60% INT, 40% STRING for one field. B1 shreds as majority INT; V2-* reject (not type-uniform).
  private static List<VariantValue> mixedTypeRows(
      int totalRows, double intRatio, String fieldName) {
    VariantMetadata meta = Variants.metadata(fieldName);
    List<VariantValue> rows = Lists.newArrayList();
    int intCount = (int) (totalRows * intRatio);
    for (int i = 0; i < totalRows; i++) {
      ShreddedObject obj = Variants.object(meta);
      if (i < intCount) {
        obj.put(fieldName, Variants.of(i));
      } else {
        obj.put(fieldName, Variants.of("str_" + i));
      }
      rows.add(obj);
    }
    return rows;
  }

  @Test
  public void testB1AcceptsMixedTypeMajority() {
    StrategyAnalyzer analyzer = new StrategyAnalyzer(InferenceStrategy.B1_MAJORITY);
    List<VariantValue> rows = mixedTypeRows(100, 0.60, "error_code");

    Type schema = analyzer.analyzeAndCreateSchema(rows, 0);
    assertThat(schema).isNotNull();

    GroupType typedValue = schema.asGroupType();
    assertThat(typedValue.containsField("error_code")).isTrue();
  }

  @Test
  public void testV2UniformRejectsMixedType() {
    StrategyAnalyzer analyzer = new StrategyAnalyzer(InferenceStrategy.V2_UNIFORM);
    List<VariantValue> rows = mixedTypeRows(100, 0.60, "error_code");

    Type schema = analyzer.analyzeAndCreateSchema(rows, 0);
    // Root is OBJECT; error_code is rejected by uniform check, no other shreddable field exists,
    // so the analyzer returns null (no typed_value worth building).
    assertThat(schema).isNull();
  }

  @Test
  public void testV2UniformAcceptsHomogeneous() {
    StrategyAnalyzer analyzer = new StrategyAnalyzer(InferenceStrategy.V2_UNIFORM);
    VariantMetadata meta = Variants.metadata("count");
    List<VariantValue> rows = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      ShreddedObject obj = Variants.object(meta);
      obj.put("count", Variants.of(i));
      rows.add(obj);
    }

    Type schema = analyzer.analyzeAndCreateSchema(rows, 0);
    assertThat(schema).isNotNull();

    GroupType typedValue = schema.asGroupType();
    assertThat(typedValue.containsField("count")).isTrue();
  }

  @Test
  public void testV2UniformPerRowDenominatorOnArrays() {
    StrategyAnalyzer analyzer = new StrategyAnalyzer(InferenceStrategy.V2_UNIFORM);
    VariantMetadata itemMeta = Variants.metadata("key");

    // 2 of 100 rows have 500-element arrays with {"key": N}. V2_UNIFORM uses per-row
    // denominator (rowsContainingPath), so key frequency is 2/100 = 2%, below 10% threshold.
    // The path is pruned - flipping the buggy B1 behavior preserved by
    // testLongArrayInFewRowsSurvivesPruning in TestVariantShreddingAnalyzer.
    List<VariantValue> rows = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      ValueArray arr = Variants.array();
      if (i < 2) {
        for (int j = 0; j < 500; j++) {
          ShreddedObject item = Variants.object(itemMeta);
          item.put("key", Variants.of(j));
          arr.add(item);
        }
      } else {
        arr.add(Variants.of("no_key"));
      }
      rows.add(arr);
    }

    Type schema = analyzer.analyzeAndCreateSchema(rows, 0);
    // Either the schema is null entirely (array element types not uniform), or the array
    // element exists without "key" shredded. Both prove V2_UNIFORM rejected "key".
    if (schema == null) {
      return;
    }
    GroupType listType = schema.asGroupType();
    GroupType repeatedGroup = listType.getType(0).asGroupType();
    GroupType elementGroup = repeatedGroup.getType(0).asGroupType();
    if (elementGroup.containsField("typed_value")) {
      GroupType elementFields = elementGroup.getType("typed_value").asGroupType();
      assertThat(elementFields.containsField("key")).isFalse();
    }
  }

  @Test
  public void testB1PerElementDenominatorOnArraysUnchanged() {
    // Regression: B1_MAJORITY must preserve the per-element bug behavior so the existing
    // testLongArrayInFewRowsSurvivesPruning in TestVariantShreddingAnalyzer keeps passing.
    // This test asserts the same outcome here with the explicit strategy constructor.
    StrategyAnalyzer analyzer = new StrategyAnalyzer(InferenceStrategy.B1_MAJORITY);
    VariantMetadata itemMeta = Variants.metadata("key");

    List<VariantValue> rows = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      ValueArray arr = Variants.array();
      if (i < 2) {
        for (int j = 0; j < 500; j++) {
          ShreddedObject item = Variants.object(itemMeta);
          item.put("key", Variants.of(j));
          arr.add(item);
        }
      } else {
        arr.add(Variants.of("no_key"));
      }
      rows.add(arr);
    }

    Type schema = analyzer.analyzeAndCreateSchema(rows, 0);
    assertThat(schema).isNotNull();

    GroupType listType = schema.asGroupType();
    GroupType repeatedGroup = listType.getType(0).asGroupType();
    GroupType elementGroup = repeatedGroup.getType(0).asGroupType();
    assertThat(elementGroup.containsField("typed_value")).isTrue();
    GroupType elementFields = elementGroup.getType("typed_value").asGroupType();
    assertThat(elementFields.containsField("key")).isTrue();
  }

  // ===== B4_FIRST_ROW (Ryan Blue strawman, 2026-06-04 sync) =====

  @Test
  public void testB4AcceptsRowZeroSchema() {
    // Row 0 has {error_code: INT}. Rows 1+ have {error_code: STRING}. B4 samples only row 0,
    // so the shredded schema is INT and the rest of the file falls back to the value field.
    StrategyAnalyzer analyzer = new StrategyAnalyzer(InferenceStrategy.B4_FIRST_ROW);
    List<VariantValue> rows = mixedTypeRows(100, 0.01, "error_code"); // 1% INT, 99% STRING

    Type schema = analyzer.analyzeAndCreateSchema(rows, 0);
    assertThat(schema).isNotNull();

    GroupType typedValue = schema.asGroupType();
    // Row 0 is INT per mixedTypeRows ordering (intCount=1 → row 0 = INT, rest = STRING).
    // B4 shreds based on row 0 only, so error_code IS shredded - and the 99 STRING rows fall
    // into the value field. This is THE failure mode Ryan's strawman exposes.
    assertThat(typedValue.containsField("error_code")).isTrue();
  }

  @Test
  public void testB4IgnoresLaterRows() {
    // Row 0 has nothing. Rows 1+ all have {error_code: INT}. B4 sees an empty schema and shreds
    // nothing, even though 99 of 100 rows would benefit.
    StrategyAnalyzer analyzer = new StrategyAnalyzer(InferenceStrategy.B4_FIRST_ROW);
    VariantMetadata meta = Variants.metadata("error_code");
    List<VariantValue> rows = Lists.newArrayList();
    rows.add(Variants.object(meta)); // row 0: empty object
    for (int i = 1; i < 100; i++) {
      ShreddedObject obj = Variants.object(meta);
      obj.put("error_code", Variants.of(i));
      rows.add(obj);
    }

    Type schema = analyzer.analyzeAndCreateSchema(rows, 0);
    // Root is OBJECT, no fields shredded (row 0 had nothing).
    assertThat(schema).isNull();
  }

  // ===== B5_FIRST_20_UNIFORM (Ryan Blue refined, with Sebastian Baunsgaard) =====

  @Test
  public void testB5AcceptsWhenFirst20AreUniform() {
    // All 100 rows have {error_code: INT}. B5 samples the first 20, sees uniform INT, shreds.
    StrategyAnalyzer analyzer = new StrategyAnalyzer(InferenceStrategy.B5_FIRST_20_UNIFORM);
    VariantMetadata meta = Variants.metadata("error_code");
    List<VariantValue> rows = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      ShreddedObject obj = Variants.object(meta);
      obj.put("error_code", Variants.of(i));
      rows.add(obj);
    }

    Type schema = analyzer.analyzeAndCreateSchema(rows, 0);
    assertThat(schema).isNotNull();

    GroupType typedValue = schema.asGroupType();
    assertThat(typedValue.containsField("error_code")).isTrue();
  }

  @Test
  public void testB5RejectsWhenFirst20MixType() {
    // First 12 rows are INT, next 8 are STRING. B5 sees the first 20, finds non-uniform, rejects.
    StrategyAnalyzer analyzer = new StrategyAnalyzer(InferenceStrategy.B5_FIRST_20_UNIFORM);
    VariantMetadata meta = Variants.metadata("error_code");
    List<VariantValue> rows = Lists.newArrayList();
    for (int i = 0; i < 12; i++) {
      ShreddedObject obj = Variants.object(meta);
      obj.put("error_code", Variants.of(i));
      rows.add(obj);
    }
    for (int i = 12; i < 100; i++) {
      ShreddedObject obj = Variants.object(meta);
      obj.put("error_code", Variants.of("str_" + i));
      rows.add(obj);
    }

    Type schema = analyzer.analyzeAndCreateSchema(rows, 0);
    assertThat(schema).isNull();
  }

  @Test
  public void testB5RejectsWhenFieldMissingFromSomeOfFirst20() {
    // First 19 rows have error_code, row 19 (the 20th) does not. B5 requires presence in all 20.
    StrategyAnalyzer analyzer = new StrategyAnalyzer(InferenceStrategy.B5_FIRST_20_UNIFORM);
    VariantMetadata meta = Variants.metadata("error_code", "other");
    List<VariantValue> rows = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      ShreddedObject obj = Variants.object(meta);
      if (i != 19) {
        obj.put("error_code", Variants.of(i));
      } else {
        obj.put("other", Variants.of(0));
      }
      rows.add(obj);
    }

    Type schema = analyzer.analyzeAndCreateSchema(rows, 0);
    // error_code appears in 19 of 20 sampled rows; B5 rejects. Root remains with no shreddable
    // fields (the OBJECT analyzer requires at least one shreddable child to emit typed_value).
    if (schema != null) {
      GroupType typedValue = schema.asGroupType();
      assertThat(typedValue.containsField("error_code")).isFalse();
    }
  }

  @Test
  public void testB5OnlySamplesFirst20() {
    // First 20 rows have {error_code: INT}, but rows 20+ have {error_code: STRING}. B5 only
    // looks at the first 20 - which are uniformly INT - so it shreds INT even though the file
    // is mostly STRING. This is the trade-off Ryan + Sebastian discussed.
    StrategyAnalyzer analyzer = new StrategyAnalyzer(InferenceStrategy.B5_FIRST_20_UNIFORM);
    VariantMetadata meta = Variants.metadata("error_code");
    List<VariantValue> rows = Lists.newArrayList();
    for (int i = 0; i < 20; i++) {
      ShreddedObject obj = Variants.object(meta);
      obj.put("error_code", Variants.of(i));
      rows.add(obj);
    }
    for (int i = 20; i < 100; i++) {
      ShreddedObject obj = Variants.object(meta);
      obj.put("error_code", Variants.of("str_" + i));
      rows.add(obj);
    }

    Type schema = analyzer.analyzeAndCreateSchema(rows, 0);
    assertThat(schema).isNotNull();

    GroupType typedValue = schema.asGroupType();
    assertThat(typedValue.containsField("error_code")).isTrue();
  }

  @Test
  public void testV2UniformWilsonAcceptsTinyNoise() {
    // 1000 rows, 998 INT + 2 STRING (99.8% uniform).
    // Wilson 95% lower bound on 998/1000 ~= 0.992 >= 0.99 -> shred.
    StrategyAnalyzer analyzer = new StrategyAnalyzer(InferenceStrategy.V2_UNIFORM_WILSON);
    VariantMetadata meta = Variants.metadata("amount");
    List<VariantValue> rows = Lists.newArrayList();
    for (int i = 0; i < 998; i++) {
      ShreddedObject obj = Variants.object(meta);
      obj.put("amount", Variants.of(i));
      rows.add(obj);
    }
    for (int i = 0; i < 2; i++) {
      ShreddedObject obj = Variants.object(meta);
      obj.put("amount", Variants.of("rogue_" + i));
      rows.add(obj);
    }

    Type schema = analyzer.analyzeAndCreateSchema(rows, 0);
    assertThat(schema).isNotNull();
    assertThat(schema.asGroupType().containsField("amount")).isTrue();
  }

  @Test
  public void testV2UniformWilsonRejectsModerateNoise() {
    // 1000 rows, 880 INT + 120 STRING (88.0% uniform).
    // Wilson 95% lower bound on 880/1000 ~= 0.858 < 0.90 -> reject.
    // V2_UNIFORM (strict) would also reject this; this test confirms Wilson at threshold 0.90
    // rejects when more than ~10% of rows have a minority type. With only "amount" rejected
    // and no other fields, the schema is null because there's nothing to shred.
    StrategyAnalyzer analyzer = new StrategyAnalyzer(InferenceStrategy.V2_UNIFORM_WILSON);
    VariantMetadata meta = Variants.metadata("amount");
    List<VariantValue> rows = Lists.newArrayList();
    for (int i = 0; i < 880; i++) {
      ShreddedObject obj = Variants.object(meta);
      obj.put("amount", Variants.of(i));
      rows.add(obj);
    }
    for (int i = 0; i < 120; i++) {
      ShreddedObject obj = Variants.object(meta);
      obj.put("amount", Variants.of("noisy_" + i));
      rows.add(obj);
    }

    Type schema = analyzer.analyzeAndCreateSchema(rows, 0);
    assertThat(schema).isNull();
  }

  @Test
  public void testV2UniformWilsonConservativeOnSmallSamples() {
    // 20 rows, all INT. p = 1.0 but n is very small. Wilson lower bound on 20/20 ~= 0.839.
    // Strategy refuses to shred because we cannot be 95% confident the true uniform fraction
    // is >= 0.90 with only 20 observations. This is the right behavior - small sample, take
    // the conservative path. With only "amount" rejected, schema is null.
    StrategyAnalyzer analyzer = new StrategyAnalyzer(InferenceStrategy.V2_UNIFORM_WILSON);
    VariantMetadata meta = Variants.metadata("amount");
    List<VariantValue> rows = Lists.newArrayList();
    for (int i = 0; i < 20; i++) {
      ShreddedObject obj = Variants.object(meta);
      obj.put("amount", Variants.of(i));
      rows.add(obj);
    }

    Type schema = analyzer.analyzeAndCreateSchema(rows, 0);
    assertThat(schema).isNull();
  }
}
