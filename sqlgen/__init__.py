"""
sqlgen — SQL script generation module for TestFileReviewAutomation.

After Phase 5 validation passes, this module generates:
  1. {client}_config.sql      — populates inflow-db-client config tables
  2. cst.DataSource{NN}_{Entity}_Load.sql — load stored procedure(s) for inflow-db-cst
  3. {client}_liquibase.xml   — Liquibase changeset XML snippet

Entry point: sqlgen.generator.generate(...)
"""
