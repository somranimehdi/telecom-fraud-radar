"""Single-page Streamlit view over exported LocalStack state."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

from dashboard.export import export


DATA_DIR = Path(__file__).resolve().parent


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _stale(data: dict) -> bool:
    value = data.get("generated_at")
    if not value:
        return True
    try:
        generated = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if generated.tzinfo is None:
            generated = generated.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - generated.astimezone(timezone.utc)).total_seconds() > 300
    except (TypeError, ValueError):
        return True


def _display_fraud_type(alert: dict) -> str:
    fraud_type = str(alert.get("fraud_type", "UNSPECIFIED"))
    if fraud_type == "UNSPECIFIED" and alert.get("rule_id") == "MODEL_GBM_V1":
        return "Suspected Fraud (Model)"
    return fraud_type


def _format_timestamp(value) -> str:
    if value is None or value == "":
        return ""
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return str(value)


def _filter_alert_rows(rows: list[dict], selection: str) -> list[dict]:
    if selection == "Model Only (MODEL_GBM_V1)":
        return [row for row in rows if row.get("rule_id") == "MODEL_GBM_V1"]
    if selection == "Rules Only":
        return [row for row in rows if row.get("rule_id") != "MODEL_GBM_V1"]
    return rows


def _display_frame(rows: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    if "fraud_type" in frame.columns:
        frame["fraud_type"] = [
            _display_fraud_type(row) for row in rows
        ]
    for column in ("created_at", "event_timestamp"):
        if column in frame.columns:
            frame[column] = frame[column].map(_format_timestamp)
    columns = [
        "created_at", "event_timestamp", "subscriber_id", "fraud_type",
        "rule_id", "severity", "status", "score",
    ]
    return frame[[column for column in columns if column in frame.columns]]


def _load_state(force_refresh: bool = False) -> tuple[dict, dict, str | None]:
    alerts_path, quality_path = DATA_DIR / "alerts.json", DATA_DIR / "quality.json"
    alerts = _read_json(alerts_path) if alerts_path.exists() else {}
    quality = _read_json(quality_path) if quality_path.exists() else {}
    warning = None
    if force_refresh or _stale(alerts) or _stale(quality):
        try:
            export(DATA_DIR)
            alerts, quality = _read_json(alerts_path), _read_json(quality_path)
        except Exception as exc:
            warning = f"Could not refresh LocalStack data ({type(exc).__name__}: {exc}). Showing available cached data."
    return alerts, quality, warning


def main() -> None:
    st.set_page_config(page_title="Telecom Fraud Radar", layout="wide")
    st.title("Telecom Fraud Radar — Live Demo")
    st.caption("LocalStack pipeline • all data simulated • Streamlit dashboard")
    refresh_now = st.button("Refresh now")
    alerts, quality, warning = _load_state(refresh_now)
    if warning:
        st.warning(warning)

    generated = alerts.get("generated_at")
    refreshed = "unknown"
    if generated:
        try:
            refreshed = datetime.fromisoformat(generated.replace("Z", "+00:00")).astimezone().strftime("%H:%M:%S")
        except ValueError:
            pass
    st.caption(f"Data refreshed at {refreshed}")

    quarantine = quality.get("overall_quarantine_rate")
    quarantine_value = "—" if quarantine is None else f"{float(quarantine):.2%}"
    m1, m2, m3 = st.columns(3)
    m1.metric("Total alerts", alerts.get("total_alerts", 0))
    m2.metric("Quarantine rate", quarantine_value)
    m3.metric("Model alerts today", alerts.get("model_alerts_today", 0))

    left, right = st.columns(2)
    with left:
        st.subheader("Alerts by fraud type")
        by_fraud_type = alerts.get("by_fraud_type", {})
        if by_fraud_type:
            st.bar_chart(pd.DataFrame({"Alerts": by_fraud_type}))
        else:
            st.info("No alert counts available.")
    with right:
        st.subheader("Quarantine rate by source")
        source_rows = quality.get("sources", [])
        source_data = {
            str(row.get("source", "unknown")): float(row.get("quarantine_rate", 0) or 0)
            for row in source_rows
        }
        if source_data:
            st.bar_chart(pd.DataFrame({"Quarantine rate": source_data}))
        else:
            st.info("No quality-gate source metrics available.")

    selection = st.sidebar.radio(
        "Alert source",
        ["All Alerts", "Model Only (MODEL_GBM_V1)", "Rules Only"],
    )

    top_heading = "Top scored alerts by rule" if selection == "Rules Only" else "Top 10 alerts by score"
    st.subheader(f"{top_heading} — {selection}")
    top = _filter_alert_rows(alerts.get("top_10_by_score", []), selection)
    if top:
        top_rows = top if selection == "Rules Only" else top[:10]
        top_frame = _display_frame(top_rows)
        if "severity" in top_frame.columns:
            styled = top_frame.style.apply(
                lambda row: ["background-color: #ffe2e2" if row.get("severity") == "HIGH" else "" for _ in row],
                axis=1,
            )
            st.dataframe(styled, use_container_width=True)
        else:
            st.dataframe(top_frame, use_container_width=True)
    else:
        st.info("No model or rule alerts to display.")

    st.subheader("Recent alerts")
    recent = _filter_alert_rows(alerts.get("recent_alerts", []), selection)
    if selection != "Rules Only":
        recent = recent[:20]
    if recent:
        recent_frame = _display_frame(recent)
        st.dataframe(recent_frame, use_container_width=True)
    else:
        st.info("No recent alerts available.")

    with st.expander("What's simulated and what's not"):
        st.markdown(
            "- All data is synthetic; labels are injected by the simulator.\n"
            "- Runs on LocalStack, not real AWS.\n"
            "- Scoring is daily batch, not event-streaming.\n"
            "- Bundle abuse detection is a proxy (source 6 deferred).\n"
            "- Model PR-AUC: 0.69 overall, 0.80 on SIM swap, 0.29 on IRSF, "
            "0.02 on bundle abuse."
        )

    time.sleep(10)
    st.rerun()


if __name__ == "__main__":
    main()
