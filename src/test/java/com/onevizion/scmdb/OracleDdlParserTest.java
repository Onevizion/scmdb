package com.onevizion.scmdb;

import com.onevizion.scmdb.model.DefaultKind;
import com.onevizion.scmdb.model.GraphqlScalar;
import com.onevizion.scmdb.model.TableMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OracleDdlParserTest {
    private final OracleDdlParser parser = new OracleDdlParser();

    @Test
    void parsesOracleTypesNullabilityPrecisionLengthDefaultsAndComments() {
        TableMetadata table = parse("""
                ITEM_ID NUMBER NOT NULL,
                SMALL_COUNT NUMBER(8,0),
                AMOUNT NUMBER(12,2),
                ITEM_NAME VARCHAR2(255 CHAR) NOT NULL,
                CREATED_DATE DATE,
                UPDATED_TS TIMESTAMP(6),
                PLAIN_VALUE NUMBER DEFAULT 1 NOT NULL,
                ON_NULL_VALUE NUMBER DEFAULT ON NULL 0 NOT NULL,
                OPTIONAL_VALUE VARCHAR2(10) DEFAULT NULL,
                CONSTRAINT PK_SAMPLE PRIMARY KEY (ITEM_ID)
                """, """
                COMMENT ON TABLE SAMPLE IS 'Sample metadata';
                COMMENT ON COLUMN SAMPLE.ITEM_NAME IS 'Item name';
                """);

        assertEquals(List.of("ITEM_ID"), table.primaryKey());
        assertEquals("Sample metadata", table.description());
        assertEquals(GraphqlScalar.ID, table.column("ITEM_ID").graphqlScalar());
        assertFalse(table.column("ITEM_ID").nullable());
        assertEquals(GraphqlScalar.INT, table.column("SMALL_COUNT").graphqlScalar());
        assertEquals(8, table.column("SMALL_COUNT").precision());
        assertEquals(0, table.column("SMALL_COUNT").scale());
        assertEquals(GraphqlScalar.DECIMAL, table.column("AMOUNT").graphqlScalar());
        assertEquals(255, table.column("ITEM_NAME").maxLength());
        assertEquals("Item name", table.column("ITEM_NAME").description());
        assertEquals(GraphqlScalar.DATE, table.column("CREATED_DATE").graphqlScalar());
        assertEquals(GraphqlScalar.DATE_TIME, table.column("UPDATED_TS").graphqlScalar());
        assertEquals(DefaultKind.DEFAULT, table.column("PLAIN_VALUE").defaultValue().kind());
        assertEquals(DefaultKind.DEFAULT_ON_NULL, table.column("ON_NULL_VALUE").defaultValue().kind());
        assertEquals("0", table.column("ON_NULL_VALUE").defaultValue().normalizedValue());
        assertNull(table.column("OPTIONAL_VALUE").defaultValue());
    }

    @Test
    void parsesBalancedChecksAndDerivesBooleanRangeAndNotEqualConstraints() {
        TableMetadata table = parse("""
                IS_ACTIVE NUMBER NOT NULL,
                PERCENT_COMPLETE NUMBER,
                PROGRAM_ID NUMBER,
                STATUS NUMBER,
                CONSTRAINT CHK_ACTIVE CHECK (NVL(IS_ACTIVE, 0) IN (0, 1)),
                CONSTRAINT CHK_PERCENT CHECK (PERCENT_COMPLETE BETWEEN 0 AND 100),
                CONSTRAINT CHK_PROGRAM CHECK (PROGRAM_ID <> 0),
                CONSTRAINT CHK_NESTED CHECK (COALESCE(STATUS, 0) <> 9 AND NVL(IS_ACTIVE, 0) = 1)
                """, "");

        assertEquals(4, table.checks().size());
        assertEquals(GraphqlScalar.BOOLEAN, table.column("IS_ACTIVE").graphqlScalar());
        assertEquals(List.of("0", "1"), table.column("IS_ACTIVE").constraints().allowedValues());
        assertEquals("0", table.column("PERCENT_COMPLETE").constraints().minimum());
        assertEquals("100", table.column("PERCENT_COMPLETE").constraints().maximum());
        assertEquals("0", table.column("PROGRAM_ID").constraints().notEqual());
        assertEquals(List.of("IS_ACTIVE", "STATUS"), table.checks().get(3).referencedColumns());
        assertEquals("COALESCE(STATUS, 0) <> 9 AND NVL(IS_ACTIVE, 0) = 1",
                table.checks().get(3).expression());
    }

    @Test
    void parsesSingleAndCompositeForeignKeysWithoutInventingLabelConvention() {
        TableMetadata table = parse("""
                SAMPLE_ID NUMBER,
                PARENT_ID NUMBER,
                TENANT_ID NUMBER,
                DISPLAY_LABEL_ID NUMBER,
                CONSTRAINT FK_PARENT FOREIGN KEY (PARENT_ID) REFERENCES PARENT (PARENT_ID),
                CONSTRAINT FK_TENANT FOREIGN KEY (SAMPLE_ID, TENANT_ID) REFERENCES TENANT_ITEM (ITEM_ID, TENANT_ID)
                """, "");

        assertEquals(2, table.foreignKeys().size());
        assertFalse(table.foreignKeys().get(0).composite());
        assertEquals("FK_PARENT", table.foreignKeys().get(0).constraintName());
        assertEquals(List.of("SAMPLE_ID", "TENANT_ID"), table.foreignKeys().get(1).sourceColumns());
        assertEquals(List.of("ITEM_ID", "TENANT_ID"), table.foreignKeys().get(1).targetColumns());
        assertTrue(table.foreignKeys().get(1).composite());
    }

    @Test
    void parsesPrimaryKeyFromSubsequentAlterTableStatement() {
        TableMetadata singleColumn = parser.parse("""
                CREATE TABLE SAMPLE (
                  SAMPLE_ID NUMBER NOT NULL,
                  SAMPLE_NAME VARCHAR2(100)
                );
                ALTER TABLE SAMPLE
                  ADD CONSTRAINT PK_SAMPLE
                  PRIMARY KEY (SAMPLE_ID);
                """);
        assertEquals(List.of("SAMPLE_ID"), singleColumn.primaryKey());

        TableMetadata composite = parser.parse("""
                CREATE TABLE SAMPLE (
                  APP_LANG_ID NUMBER NOT NULL,
                  LABEL_SYSTEM_ID NUMBER NOT NULL
                );
                ALTER TABLE SAMPLE
                  ADD CONSTRAINT PK_SAMPLE
                  PRIMARY KEY (APP_LANG_ID, LABEL_SYSTEM_ID);
                """);
        assertEquals(List.of("APP_LANG_ID", "LABEL_SYSTEM_ID"), composite.primaryKey());
    }

    @Test
    void parsesGeneratedTriggerDefaultsAndConditionalAssignments() {
        TableMetadata table = parser.parse("""
                CREATE TABLE SAMPLE (
                  SAMPLE_ID NUMBER NOT NULL,
                  TABLE_NAME VARCHAR2(100),
                  PROGRAM_ID NUMBER,
                  FLAG NUMBER DEFAULT 0 NOT NULL
                );
                CREATE OR REPLACE TRIGGER BI_SAMPLE BEFORE INSERT ON SAMPLE FOR EACH ROW BEGIN
                  IF :NEW.SAMPLE_ID IS NULL THEN :NEW.SAMPLE_ID := SEQ_SAMPLE_ID.NEXTVAL; END IF;
                  IF :NEW.TABLE_NAME IS NULL THEN :NEW.TABLE_NAME := 'SAMPLE'; END IF;
                  IF :NEW.PROGRAM_ID IS NULL THEN :NEW.PROGRAM_ID := CURRENT_PROGRAM_ID(); END IF;
                  IF :NEW.FLAG IS NULL THEN :NEW.FLAG := 1; END IF;
                END;
                """);

        assertTrue(table.column("SAMPLE_ID").source().generated());
        assertTrue(table.column("SAMPLE_ID").readOnly());
        assertEquals(DefaultKind.TRIGGER, table.column("SAMPLE_ID").defaultValue().kind());
        assertEquals("SAMPLE", table.column("TABLE_NAME").defaultValue().normalizedValue());
        assertEquals("Trigger: fills value conditionally", table.column("PROGRAM_ID").source().description());
        assertTrue(table.column("PROGRAM_ID").source().environmentSpecific());
        assertEquals(DefaultKind.DEFAULT, table.column("FLAG").defaultValue().kind());
        assertNotNull(table.column("FLAG").source());
    }

    private TableMetadata parse(String definitions, String suffix) {
        return parser.parse("CREATE TABLE SAMPLE (" + definitions + ");\n" + suffix);
    }
}
