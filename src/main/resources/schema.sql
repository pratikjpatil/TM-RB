-- schemas.sql
-- H2 Database Schema for Report Builder Application

-- ============================================================
-- 1. CLEANUP (Drop existing tables to ensure clean state)
-- ============================================================
DROP TABLE IF EXISTS RB_FILTER_RULE;
DROP TABLE IF EXISTS RB_VARIANT_PARAM_DEF;
DROP TABLE IF EXISTS RB_REPORT_VARIANT;
DROP TABLE IF EXISTS RB_REPORT_TEMPLATE;
DROP TABLE IF EXISTS GL_BALANCE;
DROP TABLE IF EXISTS BRANCH_MASTER;

-- ============================================================
-- 2. CONFIGURATION TABLES (Report Builder Metadata)
-- ============================================================

-- Table: RB_REPORT_TEMPLATE
-- Stores the JSON definition of reports
CREATE TABLE RB_REPORT_TEMPLATE (
                                    TEMPLATE_ID     VARCHAR(64) NOT NULL,
                                    TEMPLATE_JSON   CLOB,                 -- Stores the huge JSON template
                                    STATUS          VARCHAR(20) DEFAULT 'ACTIVE' NOT NULL,
                                    CREATED_BY      VARCHAR(64),
                                    CREATED_AT      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                    UPDATED_BY      VARCHAR(64),
                                    UPDATED_AT      TIMESTAMP,
                                    CONSTRAINT PK_RB_REPORT_TEMPLATE PRIMARY KEY (TEMPLATE_ID)
);

-- Table: RB_REPORT_VARIANT
-- Stores specific variations (e.g., "Branch Wise" vs "Whole Bank")
CREATE TABLE RB_REPORT_VARIANT (
                                   VARIANT_ID      BIGINT NOT NULL,      -- Using Numeric ID for internal linking
                                   TEMPLATE_ID     VARCHAR(64) NOT NULL,
                                   VARIANT_CODE    VARCHAR(100) NOT NULL,
                                   VARIANT_NAME    VARCHAR(200) NOT NULL,
                                   DESCRIPTION     VARCHAR(500),
                                   STATUS          VARCHAR(20) DEFAULT 'ACTIVE',
                                   CREATED_BY      VARCHAR(64),
                                   CREATED_AT      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                   UPDATED_BY      VARCHAR(64),
                                   UPDATED_AT      TIMESTAMP,
                                   CONSTRAINT PK_RB_REPORT_VARIANT PRIMARY KEY (VARIANT_ID),
                                   CONSTRAINT FK_VARIANT_TEMPLATE FOREIGN KEY (TEMPLATE_ID)
                                       REFERENCES RB_REPORT_TEMPLATE (TEMPLATE_ID),
                                   CONSTRAINT UQ_VARIANT_CODE UNIQUE (TEMPLATE_ID, VARIANT_CODE)
);

-- Table: RB_VARIANT_PARAM_DEF
-- Defines the input parameters required for a specific variant (e.g., REPORT_DATE)
CREATE TABLE RB_VARIANT_PARAM_DEF (
                                      PARAM_ID         BIGINT NOT NULL,
                                      VARIANT_ID       BIGINT NOT NULL,
                                      PARAM_NAME       VARCHAR(100) NOT NULL,
                                      LABEL            VARCHAR(200),
                                      PARAM_TYPE       VARCHAR(20),       -- STRING, DATE, NUMBER
                                      REQUIRED_YN      CHAR(1) DEFAULT 'Y',
                                      MULTI_VALUED_YN  CHAR(1) DEFAULT 'N',
                                      VALIDATION_REGEX VARCHAR(500),
                                      UI_HINT          VARCHAR(100),      -- e.g., 'date', 'text'
                                      CREATED_AT       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                      CONSTRAINT PK_RB_VARIANT_PARAM_DEF PRIMARY KEY (PARAM_ID),
                                      CONSTRAINT FK_PARAM_VARIANT FOREIGN KEY (VARIANT_ID)
                                          REFERENCES RB_REPORT_VARIANT (VARIANT_ID)
);

-- Table: RB_FILTER_RULE
-- Maps input parameters to actual DB column filters (Hidden logic)
CREATE TABLE RB_FILTER_RULE (
                                RULE_ID         BIGINT NOT NULL,
                                VARIANT_ID      BIGINT NOT NULL,
                                SCOPE_TYPE      VARCHAR(50) NOT NULL, -- ALL_DB, TABLE, DYNAMIC_TABLE
                                SCOPE_VALUE     VARCHAR(200),         -- Table Name (optional)
                                PARAM_NAME      VARCHAR(100) NOT NULL,
                                DB_COLUMN       VARCHAR(100) NOT NULL,
                                OPERATOR        VARCHAR(20) NOT NULL, -- =, IN, LIKE, etc.
                                CREATED_AT      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                CONSTRAINT PK_RB_FILTER_RULE PRIMARY KEY (RULE_ID),
                                CONSTRAINT FK_RULE_VARIANT FOREIGN KEY (VARIANT_ID)
                                    REFERENCES RB_REPORT_VARIANT (VARIANT_ID)
);

-- ============================================================
-- 3. BUSINESS DATA TABLES (Mock Data Targets)
-- ============================================================

-- Table: BRANCH_MASTER
-- Reference table for Branch details
CREATE TABLE BRANCH_MASTER (
                               CODE            VARCHAR(20) NOT NULL,
                               NAME            VARCHAR(100),
                               CIRCLE_CODE     VARCHAR(20),
                               REGION_CODE     VARCHAR(20),
                               STATUS          VARCHAR(20),
                               CONSTRAINT PK_BRANCH_MASTER PRIMARY KEY (CODE)
);

-- Table: GL_BALANCE
-- Transactional data table for Report Generation
CREATE TABLE GL_BALANCE (
                            ID              BIGINT NOT NULL,
                            BALANCE_DATE    DATE NOT NULL,
                            BRANCH_CODE     VARCHAR(20) NOT NULL,
                            CURRENCY        VARCHAR(3) NOT NULL,
                            CGL             VARCHAR(20) NOT NULL,  -- Common General Ledger Code
                            BALANCE         DECIMAL(25,4) NOT NULL,
                            INR_BALANCE     DECIMAL(25,2),
                            CONSTRAINT PK_GL_BALANCE PRIMARY KEY (ID)
);

-- ============================================================
-- 4. INDEXES (Performance Optimization)
-- ============================================================
CREATE INDEX IDX_GL_BAL_BR_DT ON GL_BALANCE(BRANCH_CODE, BALANCE_DATE);
CREATE INDEX IDX_GL_BAL_CGL ON GL_BALANCE(CGL);