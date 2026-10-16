from __future__ import annotations

import duckdb

from ukam_os_builder.data_sources.abp.transform.stages.combine import (
    combine_and_dedupe,
)


def test_abp_output_includes_local_custodian_code() -> None:
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE _stage_lpi_variants AS
        SELECT
            100000000001::BIGINT AS uprn,
            'E8 1EA'::VARCHAR AS postcode,
            '1 TEST STREET'::VARCHAR AS raw_address,
            'LPI'::VARCHAR AS source,
            1::INTEGER AS logical_status,
            '2'::VARCHAR AS blpu_state,
            'D'::VARCHAR AS postal_address_code,
            NULL::BIGINT AS parent_uprn,
            'S'::VARCHAR AS hierarchy_level,
            'PRIMARY'::VARCHAR AS variant_label,
            TRUE AS is_primary
    """)
    for table_name in (
        "_stage_business_variants",
        "_stage_delivery_point_variants",
        "_stage_custom_level_variants",
    ):
        con.execute(
            f"CREATE TABLE {table_name} AS "
            "SELECT * FROM _stage_lpi_variants WHERE FALSE"
        )

    con.execute("""
        CREATE TABLE classification_best (
            uprn BIGINT,
            classification_code VARCHAR
        );
        CREATE TABLE delivery_point_best (
            uprn BIGINT,
            udprn BIGINT
        );
        CREATE TABLE blpu (
            uprn BIGINT,
            local_custodian_code INTEGER
        );
        INSERT INTO blpu VALUES (100000000001, 5360);
    """)

    result = combine_and_dedupe(con)

    assert result.project("unique_id, local_custodian_code").fetchone() == (
        100000000001,
        5360,
    )
    con.close()
