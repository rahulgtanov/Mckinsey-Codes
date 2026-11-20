"""
Daily DAG Status Report by Email.

Runs once per day at 6 AM UTC, collects DAG run statuses for the last 24 hours,
and sends a summary by email (success / failed / running counts)
and a list of failed scheduled DAGs only.

Uses Airflow Variables: ENV, EMAIL_RECEIPIENTS (same as other DAGs).
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable, DagModel, TaskInstance
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import DagRun
from airflow.utils.state import State
from airflow.utils.session import provide_session
from airflow.utils import timezone
from email_script import send_email
from sqlalchemy import func

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "catchup": False,
}


@provide_session
def _build_failed_scheduled_dags_list(session=None):
    """
    Return only scheduled DAGs (schedule_interval is not null) whose latest run state is failed.
    Returns HTML: highlighted list of failed scheduled DAGs with schedule and last run time.
    """
    # Scheduled DAGs: DagModel where schedule_interval is not null (and not paused)
    scheduled = (
        session.query(DagModel.dag_id, DagModel.schedule_interval)
        .filter(DagModel.is_paused == False)
        .filter(DagModel.schedule_interval.isnot(None))
        .all()
    )
    if not scheduled:
        return "<p>No scheduled DAGs found.</p>", 0

    dag_ids = [d.dag_id for d in scheduled]
    schedule_by_dag = {d.dag_id: str(d.schedule_interval) for d in scheduled}

    # Latest run per DAG (by execution_date)
    subq = (
        session.query(DagRun.dag_id, func.max(DagRun.execution_date).label("last_exec"))
        .filter(DagRun.dag_id.in_(dag_ids))
        .group_by(DagRun.dag_id)
    ).subquery()

    latest_runs = (
        session.query(DagRun)
        .join(
            subq,
            (DagRun.dag_id == subq.c.dag_id) & (DagRun.execution_date == subq.c.last_exec),
        )
        .all()
    )
    status_by_dag = {r.dag_id: (r.state or "unknown", r.execution_date) for r in latest_runs}

    # Keep only failed scheduled DAGs that actually have a schedule (exclude None/empty)
    failed_rows = []
    for dag_id in sorted(dag_ids):
        schedule = schedule_by_dag.get(dag_id, "")
        if not schedule or str(schedule).strip().lower() == "none":
            continue
        state, exec_dt = status_by_dag.get(dag_id, ("no run", None))
        if state != State.FAILED:
            continue
        last_run_str = exec_dt.strftime("%Y-%m-%d %H:%M UTC") if exec_dt else "—"
        # Get task IDs that failed in this DAG run
        failed_tasks = (
            session.query(TaskInstance.task_id)
            .filter(TaskInstance.dag_id == dag_id)
            .filter(TaskInstance.execution_date == exec_dt)
            .filter(TaskInstance.state == State.FAILED)
            .all()
        )
        failed_task_ids = ", ".join(t.task_id for t in failed_tasks) if failed_tasks else "—"
        failed_rows.append((dag_id, schedule, last_run_str, failed_task_ids))

    if not failed_rows:
        return "<p>No failed scheduled DAGs.</p>", 0

    # Table: DAG ID | Schedule | Last run (UTC) | Tasks failed
    table_rows = "".join(
        '<tr style="background-color: #ffcccc;">'
        + "<td><strong>{}</strong></td><td>{}</td><td>{}</td><td>{}</td></tr>".format(
            dag_id, schedule, last_run_str, failed_task_ids
        )
        for dag_id, schedule, last_run_str, failed_task_ids in failed_rows
    )
    html = """
        <table border="1" cellpadding="6" cellspacing="0" style="border-collapse: collapse;">
            <tr><th>DAG ID</th><th>Schedule</th><th>Last run (UTC)</th><th>Tasks failed</th></tr>
            {}
        </table>
    """.format(
        table_rows
    )
    return html, len(failed_rows)


@provide_session
def build_and_send_daily_report(session=None, **context):
    """
    Query DagRuns from the last 24 hours, build a summary, and send by email.
    """
    env = Variable.get("ENV", default_var="Airflow")
    recipients = Variable.get("EMAIL_RECEIPIENTS", default_var="")
    if recipients:
        recipients = [r.strip() for r in recipients.split(",") if r.strip()]

    if not recipients:
        raise ValueError("EMAIL_RECEIPIENTS must be set in Airflow Variables")

    # Last 24 hours by execution_date (runs that were scheduled in this window)
    # Use timezone-aware datetimes (Airflow SQLAlchemy disallows naive datetime)
    report_end = timezone.utcnow()
    report_start = report_end - timedelta(hours=24)

    query = (
        session.query(DagRun)
        .filter(DagRun.execution_date >= report_start)
        .filter(DagRun.execution_date < report_end)
    )
    dag_runs = query.all()

    by_state = {}
    for run in dag_runs:
        state = run.state or "unknown"
        by_state[state] = by_state.get(state, 0) + 1

    success_count = by_state.get(State.SUCCESS, 0)
    failed_count = by_state.get(State.FAILED, 0)
    running_count = by_state.get(State.RUNNING, 0)
    other_count = sum(
        c for s, c in by_state.items()
        if s not in (State.SUCCESS, State.FAILED, State.RUNNING)
    )

    # Build list of failed scheduled DAGs only (last run status = failed)
    failed_scheduled_dags_html, failed_scheduled_count = _build_failed_scheduled_dags_list(session)

    report_date = report_end.strftime("%Y-%m-%d %H:%M UTC")
    subject = f"[Airflow {env}] Daily DAG Status Report - {report_date}"
    failed_cell_style = ' style="background-color: #ffcccc; font-weight: bold;"' if failed_count else ""

    html_content = """
    <!DOCTYPE html>
    <html>
    <head></head>
    <body>
        <h1>Daily DAG Status Report [{env}]</h1>
        <p><strong>Report time:</strong> {report_date}</p>
        <p><strong>Period:</strong> Last 24 hours</p>
        <hr/>
        <h2>Summary</h2>
        <p><em>Counts are <strong>number of DAG runs</strong> in the last 24 hours (same DAG can have multiple runs).</em></p>
        <table border="1" cellpadding="6" cellspacing="0">
            <tr><th>Success</th><th>Failed</th><th>Running</th><th>Other</th></tr>
            <tr><td>{success_count}</td><td{failed_cell_style}>{failed_count}</td><td>{running_count}</td><td>{other_count}</td></tr>
        </table>
        <h2>Failed scheduled DAGs</h2>
        <p><em>Scheduled DAGs whose <strong>latest run</strong> failed (one row per DAG; total = {failed_scheduled_count} DAGs).</em></p>
        {failed_scheduled_dags_html}
    </body>
    </html>
    """.format(
        env=env,
        report_date=report_date,
        success_count=success_count,
        failed_cell_style=failed_cell_style,
        failed_count=failed_count,
        running_count=running_count,
        other_count=other_count,
        failed_scheduled_count=failed_scheduled_count,
        failed_scheduled_dags_html=failed_scheduled_dags_html,
    )

    send_email(subject, html_content, recipients)


with DAG(
    dag_id="Dag_Daily_Status_Report",
    default_args=default_args,
    schedule_interval="0 */4 * * *",  # Daily at 06:00 UTC
    tags=["reporting", "email", "daily"],
) as dag:
    send_report = PythonOperator(
        task_id="send_daily_dag_status_email",
        python_callable=build_and_send_daily_report,
        provide_context=True,
    )