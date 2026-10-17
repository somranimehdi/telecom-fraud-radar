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

THEME_CSS = """
<style>
:root {
  --dq-cyan: #00f0ff;
  --dq-lime: #b7ff3c;
  --dq-pink: #ff3cac;
  --dq-panel: #11151a;
  --dq-muted: #84919c;
}

.stApp,
[data-testid="stAppViewContainer"] {
  background:
    radial-gradient(ellipse at 78% 0%, rgba(0, 240, 255, .09), transparent 35%),
    radial-gradient(ellipse at 8% 35%, rgba(255, 60, 172, .055), transparent 34%),
    #0a0a0a;
  color: #e0e0e0;
  font-family: "SFMono-Regular", Consolas, "Liberation Mono", monospace;
}

[data-testid="stHeader"] {
  background: rgba(10, 10, 10, .86);
}

[data-testid="stAppViewContainer"] > .main {
  background-image: repeating-linear-gradient(
    to bottom, transparent 0, transparent 3px, rgba(255,255,255,.012) 4px
  );
}

.block-container {
  max-width: 1440px;
  padding-top: 2.2rem;
  padding-bottom: 3rem;
}

.dq-hero {
  position: relative;
  overflow: hidden;
  padding: 1.5rem 1.8rem 1.3rem;
  margin: 0 0 1.4rem;
  background: linear-gradient(105deg, #11151a 0%, #101116 64%, #15101a 100%);
  border: 1px solid rgba(0,240,255,.65);
  border-left: 5px solid var(--dq-cyan);
  box-shadow: 0 0 24px rgba(0,240,255,.09), inset 0 0 24px rgba(0,240,255,.025);
}

.dq-hero::after {
  content: "DQ // 001";
  position: absolute;
  right: 1.5rem;
  bottom: .8rem;
  color: rgba(0,240,255,.16);
  font-size: 2rem;
  font-weight: 900;
  letter-spacing: .12em;
}

.dq-eyebrow {
  color: var(--dq-lime);
  font-size: .72rem;
  letter-spacing: .2em;
  text-transform: uppercase;
}

.dq-hero h1 {
  color: #f1fbff;
  font-size: clamp(2rem, 5vw, 3.15rem);
  letter-spacing: .09em;
  line-height: 1.1;
  margin: .45rem 0 .4rem;
  text-shadow: 0 0 16px rgba(0,240,255,.35);
}

.dq-hero p {
  color: #9aa8b2;
  font-size: .82rem;
  letter-spacing: .11em;
  margin: 0;
  text-transform: uppercase;
}

h2, h3 {
  color: #e8faff;
  letter-spacing: .055em;
}

[data-testid="stMetric"] {
  background: linear-gradient(145deg, #14191f, #0f1115);
  border: 1px solid rgba(0,240,255,.4);
  border-bottom: 2px solid rgba(0,240,255,.75);
  border-radius: 3px;
  box-shadow: 0 5px 22px rgba(0,0,0,.35), inset 0 0 18px rgba(0,240,255,.025);
  padding: 1rem 1.1rem;
}

[data-testid="stMetricLabel"] p {
  color: var(--dq-muted);
  font-size: .7rem;
  letter-spacing: .12em;
  text-transform: uppercase;
}

[data-testid="stMetricValue"] {
  color: var(--dq-cyan);
  text-shadow: 0 0 12px rgba(0,240,255,.28);
}

[data-testid="stSidebar"] {
  background: #0e1115;
  border-right: 1px solid rgba(0,240,255,.38);
}

[data-testid="stSidebar"] h2 {
  color: var(--dq-cyan);
  font-size: 1rem;
  letter-spacing: .14em;
}

[data-testid="stRadio"] label p {
  color: #d9e1e6;
  font-size: .86rem;
}

button[kind="secondary"], [data-testid="stBaseButton-secondary"] {
  border: 1px solid var(--dq-cyan) !important;
  border-radius: 2px !important;
  background: #10171b !important;
  color: var(--dq-cyan) !important;
  font-family: inherit !important;
  letter-spacing: .08em;
  text-transform: uppercase;
}

button[kind="secondary"]:hover, [data-testid="stBaseButton-secondary"]:hover {
  background: rgba(0,240,255,.12) !important;
  box-shadow: 0 0 15px rgba(0,240,255,.22);
}

[data-testid="stDataFrame"] {
  border: 1px solid rgba(0,240,255,.35);
  border-radius: 3px;
  box-shadow: 0 0 18px rgba(0,0,0,.35);
}

[data-testid="stVegaLiteChart"], [data-testid="stArrowVegaLiteChart"] {
  background: rgba(17,21,26,.72);
  border: 1px solid rgba(0,240,255,.22);
  border-radius: 3px;
  padding: .35rem;
}

[data-testid="stCaptionContainer"] p {
  color: #82909b;
  letter-spacing: .035em;
}

hr { border-color: rgba(0,240,255,.22); }
</style>
"""


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
    st.markdown(THEME_CSS, unsafe_allow_html=True)
    st.markdown(
        """
        <section class="dq-hero">
          <div class="dq-eyebrow">SYS.001 // FRAUD MONITORING GRID</div>
          <h1>DATA QUEST</h1>
          <p>Telecom Fraud Radar &nbsp;·&nbsp; LocalStack Simulation</p>
        </section>
        """,
        unsafe_allow_html=True,
    )
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
    st.caption(f"SYNC STATUS // DATA REFRESHED AT {refreshed}")

    quarantine = quality.get("overall_quarantine_rate")
    quarantine_value = "—" if quarantine is None else f"{float(quarantine):.2%}"
    m1, m2, m3 = st.columns(3)
    m1.metric("SYS.002 // TOTAL ALERTS", alerts.get("total_alerts", 0))
    m2.metric("SYS.003 // QUARANTINE RATE", quarantine_value)
    m3.metric("SYS.004 // MODEL ALERTS TODAY", alerts.get("model_alerts_today", 0))

    left, right = st.columns(2)
    with left:
        st.subheader("SYS.005 // ALERTS BY FRAUD TYPE")
        by_fraud_type = alerts.get("by_fraud_type", {})
        if by_fraud_type:
            st.bar_chart(pd.DataFrame({"Alerts": by_fraud_type}))
        else:
            st.info("No alert counts available.")
    with right:
        st.subheader("SYS.006 // QUARANTINE BY SOURCE")
        source_rows = quality.get("sources", [])
        source_data = {
            str(row.get("source", "unknown")): float(row.get("quarantine_rate", 0) or 0)
            for row in source_rows
        }
        if source_data:
            st.bar_chart(pd.DataFrame({"Quarantine rate": source_data}))
        else:
            st.info("No quality-gate source metrics available.")

    st.sidebar.markdown("## CONTROL DECK")
    st.sidebar.caption("SYS.007 // ALERT SOURCE FILTER")
    selection = st.sidebar.radio(
        "Alert source",
        ["All Alerts", "Model Only (MODEL_GBM_V1)", "Rules Only"],
    )

    top_heading = "Top scored alerts by rule" if selection == "Rules Only" else "Top 10 alerts by score"
    st.subheader(f"SYS.008 // {top_heading} — {selection.upper()}")
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

    st.subheader("SYS.009 // RECENT ALERTS")
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
