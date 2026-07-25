from __future__ import annotations

import duckdb

from .dual_theme_common import (
    P0Partition,
    _path_value,
    _sql_path,
    _theme_id_expression,
    factor_id,
)


def _partition_metadata(
    connection: duckdb.DuckDBPyConnection,
    partition: P0Partition,
) -> dict[str, object]:
    edges = _sql_path(partition.edges)
    row = connection.execute(
        f"""
        SELECT
          any_value(CAST(trade_date AS VARCHAR)),
          any_value(CAST(layer_id AS VARCHAR)),
          any_value(CAST(scale_minutes AS INTEGER)),
          count(*),
          coalesce(sum(CASE WHEN edge_available_time > decision_time THEN 1 ELSE 0 END), 0)
        FROM read_parquet('{edges}', union_by_name=true)
        """
    ).fetchone()
    trade_date = (
        str(row[0]) if row[0] is not None else _path_value(partition.edges, "date")
    )
    layer_id = (
        str(row[1]) if row[1] is not None else _path_value(partition.edges, "layer")
    )
    scale_raw = row[2] if row[2] is not None else _path_value(partition.edges, "scale")
    if not trade_date or not layer_id or scale_raw is None:
        raise ValueError(f"Cannot resolve partition identity: {partition.edges}")
    return {
        "trade_date": trade_date,
        "layer_id": layer_id,
        "scale_minutes": int(scale_raw),
        "edge_rows": int(row[3]),
        "pit_violations": int(row[4] or 0),
    }


def _signal_header(
    *,
    batch_id: str,
    scope: str,
    theme_family: str,
    layer_id: str,
    variant: str,
) -> str:
    return f"""
      '{batch_id}'::VARCHAR AS batch_id,
      '{factor_id(scope, theme_family, layer_id, variant)}'::VARCHAR AS factor_id,
      '{scope}'::VARCHAR AS scope,
      '{theme_family}'::VARCHAR AS theme_family,
      '{layer_id}'::VARCHAR AS layer_id,
    """


def _stock_query(
    *,
    batch_id: str,
    partition: P0Partition,
    meta: dict[str, object],
    variant: str,
) -> str:
    edges_path = _sql_path(partition.edges)
    nodes_path = _sql_path(partition.nodes)
    membership_path = (
        _sql_path(partition.scope_memberships)
        if partition.scope_memberships is not None
        else None
    )
    layer = str(meta["layer_id"])
    trade_date = str(meta["trade_date"])
    header = _signal_header(
        batch_id=batch_id,
        scope=partition.scope,
        theme_family=partition.theme_family,
        layer_id=layer,
        variant=variant,
    )
    membership_cte = ""
    membership_join = ""
    context_theme = "NULL::VARCHAR"
    membership_weight = "NULL::DOUBLE"
    membership_group = ""
    if partition.scope == "within_theme":
        if membership_path is None:
            raise ValueError("Within-theme export requires canonical scope memberships")
        membership_cte = f"""
        , memberships AS (
          SELECT
            CAST(trade_date AS VARCHAR) AS trade_date,
            decision_time,
            CAST(theme_id AS VARCHAR) AS theme_id,
            CAST(symbol_id AS BIGINT) AS symbol_id,
            CAST(membership_weight AS DOUBLE) AS membership_weight
          FROM read_parquet('{membership_path}', union_by_name=true)
        )
        """
        membership_join = """
        JOIN memberships membership
          ON CAST(e.trade_date AS VARCHAR)=membership.trade_date
         AND e.decision_time=membership.decision_time
         AND {selected_id}=membership.symbol_id
        """
        context_theme = "membership.theme_id"
        membership_weight = "membership.membership_weight"
        membership_group = ", membership.theme_id, membership.membership_weight"
    if variant == "node_baseline":
        node_membership_cte = ""
        node_membership_join = ""
        node_context = "NULL::VARCHAR"
        node_weight = "NULL::DOUBLE"
        if partition.scope == "within_theme":
            if membership_path is None:
                raise ValueError("Within-theme export requires canonical scope memberships")
            node_membership_cte = f"""
            WITH memberships AS (
              SELECT
                CAST(trade_date AS VARCHAR) AS trade_date,
                decision_time,
                CAST(theme_id AS VARCHAR) AS theme_id,
                CAST(symbol_id AS BIGINT) AS symbol_id,
                CAST(membership_weight AS DOUBLE) AS membership_weight
              FROM read_parquet('{membership_path}', union_by_name=true)
            )
            """
            node_membership_join = """
            JOIN memberships membership
              ON CAST(nodes.trade_date AS VARCHAR)=membership.trade_date
             AND nodes.decision_time=membership.decision_time
             AND nodes.symbol_id=membership.symbol_id
            """
            node_context = "membership.theme_id"
            node_weight = "membership.membership_weight"
        return f"""
        {node_membership_cte}
        SELECT {header}
          CAST(nodes.scale_minutes AS INTEGER) AS scale_minutes,
          '{variant}'::VARCHAR AS variant_id,
          nodes.trade_date,
          nodes.decision_time,
          nodes.p0_snapshot_id,
          nodes.symbol,
          nodes.symbol_id,
          nodes.security_entity_id,
          nodes.node_score::DOUBLE AS score,
          0::DOUBLE AS own_score,
          0::BIGINT AS edge_count,
          {node_context} AS context_theme_id,
          {node_weight} AS membership_weight,
          nodes.decision_time AS signal_available_time
        FROM read_parquet('{nodes_path}', union_by_name=true) nodes
        {node_membership_join}
        WHERE nodes.node_score IS NOT NULL
          AND CAST(nodes.trade_date AS VARCHAR)='{trade_date}'
        """
    if variant == "graph_forward":
        selected_symbol = "any_value(dst.symbol)"
        selected_id = "e.dst_symbol_id"
        selected_entity = "any_value(dst.security_entity_id)"
        score = (
            "sum(e.edge_weight * src.node_score) / "
            "nullif(sum(abs(e.edge_weight)), 0)"
        )
        own = "any_value(dst.node_score)"
        valid = "src.node_score IS NOT NULL"
    else:
        selected_symbol = "any_value(src.symbol)"
        selected_id = "e.src_symbol_id"
        selected_entity = "any_value(src.security_entity_id)"
        score = (
            "sum(e.edge_weight * dst.node_score) / "
            "nullif(sum(abs(e.edge_weight)), 0)"
        )
        own = "any_value(src.node_score)"
        valid = "dst.node_score IS NOT NULL"
    scoped_join = membership_join.format(selected_id=selected_id)
    # A scoped signal cannot be available before the P1 theme membership used to
    # define its scope. Global signals retain the actual edge-availability time;
    # Within-Theme signals become available at the governed theme decision time.
    signal_available = (
        "e.decision_time"
        if partition.scope == "within_theme"
        else "max(e.edge_available_time)"
    )
    return f"""
    WITH edges AS (
      SELECT * FROM read_parquet('{edges_path}', union_by_name=true)
      WHERE edge_available_time <= decision_time AND edge_weight IS NOT NULL
    ), nodes AS (
      SELECT * FROM read_parquet('{nodes_path}', union_by_name=true)
    )
    {membership_cte}
    SELECT {header}
      CAST(any_value(e.scale_minutes) AS INTEGER) AS scale_minutes,
      '{variant}'::VARCHAR AS variant_id,
      any_value(e.trade_date) AS trade_date,
      e.decision_time,
      any_value(e.p0_snapshot_id) AS p0_snapshot_id,
      {selected_symbol} AS symbol,
      {selected_id} AS symbol_id,
      {selected_entity} AS security_entity_id,
      {score} AS score,
      {own} AS own_score,
      count(*)::BIGINT AS edge_count,
      {context_theme} AS context_theme_id,
      {membership_weight} AS membership_weight,
      {signal_available} AS signal_available_time
    FROM edges e
    JOIN nodes src
      ON e.decision_time=src.decision_time
     AND e.p0_snapshot_id=src.p0_snapshot_id
     AND e.src_symbol_id=src.symbol_id
    JOIN nodes dst
      ON e.decision_time=dst.decision_time
     AND e.p0_snapshot_id=dst.p0_snapshot_id
     AND e.dst_symbol_id=dst.symbol_id
    {scoped_join}
    WHERE {valid}
    GROUP BY e.decision_time, {selected_id}{membership_group}
    """


def _inter_query(
    *,
    batch_id: str,
    partition: P0Partition,
    meta: dict[str, object],
    variant: str,
) -> str:
    if partition.scope_memberships is None:
        raise ValueError("Inter-theme export requires scope memberships")
    edges_path = _sql_path(partition.edges)
    nodes_path = _sql_path(partition.nodes)
    membership_path = _sql_path(partition.scope_memberships)
    layer = str(meta["layer_id"])
    header = _signal_header(
        batch_id=batch_id,
        scope=partition.scope,
        theme_family=partition.theme_family,
        layer_id=layer,
        variant=variant,
    )
    if variant == "node_baseline":
        theme_scores = f"""
        SELECT
          decision_time,
          p0_snapshot_id,
          CAST(scale_minutes AS INTEGER) AS scale_minutes,
          {_theme_id_expression('nodes')} AS context_theme_id,
          node_score::DOUBLE AS score,
          0::DOUBLE AS own_score,
          0::BIGINT AS edge_count,
          decision_time AS signal_available_time
        FROM read_parquet('{nodes_path}', union_by_name=true) nodes
        WHERE node_score IS NOT NULL
        """
    else:
        if variant == "graph_forward":
            output_theme = _theme_id_expression("dst")
            score = (
                "sum(e.edge_weight * src.node_score) / "
                "nullif(sum(abs(e.edge_weight)), 0)"
            )
            own = "any_value(dst.node_score)"
            valid = "src.node_score IS NOT NULL"
        else:
            output_theme = _theme_id_expression("src")
            score = (
                "sum(e.edge_weight * dst.node_score) / "
                "nullif(sum(abs(e.edge_weight)), 0)"
            )
            own = "any_value(src.node_score)"
            valid = "dst.node_score IS NOT NULL"
        theme_scores = f"""
        WITH edges AS (
          SELECT * FROM read_parquet('{edges_path}', union_by_name=true)
          WHERE edge_available_time <= decision_time AND edge_weight IS NOT NULL
        ), nodes AS (
          SELECT * FROM read_parquet('{nodes_path}', union_by_name=true)
        )
        SELECT
          e.decision_time,
          any_value(e.p0_snapshot_id) AS p0_snapshot_id,
          CAST(any_value(e.scale_minutes) AS INTEGER) AS scale_minutes,
          {output_theme} AS context_theme_id,
          {score} AS score,
          {own} AS own_score,
          count(*)::BIGINT AS edge_count,
          e.decision_time AS signal_available_time
        FROM edges e
        JOIN nodes src
          ON e.decision_time=src.decision_time
         AND e.p0_snapshot_id=src.p0_snapshot_id
         AND e.src_symbol_id=src.symbol_id
        JOIN nodes dst
          ON e.decision_time=dst.decision_time
         AND e.p0_snapshot_id=dst.p0_snapshot_id
         AND e.dst_symbol_id=dst.symbol_id
        WHERE {valid}
        GROUP BY e.decision_time, {output_theme}
        """
    return f"""
    WITH theme_scores AS (
      {theme_scores}
    ), memberships AS (
      SELECT
        trade_date,
        decision_time,
        CAST(theme_id AS VARCHAR) AS theme_id,
        symbol,
        symbol_id,
        security_entity_id,
        CAST(membership_weight AS DOUBLE) AS membership_weight
      FROM read_parquet('{membership_path}', union_by_name=true)
    )
    SELECT {header}
      theme_scores.scale_minutes,
      '{variant}'::VARCHAR AS variant_id,
      memberships.trade_date,
      theme_scores.decision_time,
      theme_scores.p0_snapshot_id,
      memberships.symbol,
      memberships.symbol_id,
      memberships.security_entity_id,
      theme_scores.score,
      theme_scores.own_score,
      theme_scores.edge_count,
      theme_scores.context_theme_id,
      memberships.membership_weight,
      theme_scores.signal_available_time
    FROM theme_scores
    JOIN memberships
      ON theme_scores.decision_time=memberships.decision_time
     AND theme_scores.context_theme_id=memberships.theme_id
    """
