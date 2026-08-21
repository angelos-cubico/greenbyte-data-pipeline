import logging

import azure.functions as func

from download_status_logs_incremental import main as status_main
from download_signals_incremental import main as signals_main
from download_dirtydozen_incremental import main as dirtydozen_main
from generate_appendices_common_graph import main as appendices_main

app = func.FunctionApp()


def run_pipeline(label, pipeline):
    logging.warning("=" * 80)
    logging.warning("%s START", label)
    logging.warning("=" * 80)
    try:
        pipeline()
    except Exception:
        logging.exception("%s FAILED", label)
        raise
    logging.warning("=" * 80)
    logging.warning("%s COMPLETE", label)
    logging.warning("=" * 80)


# Status logs first. Independent invocation, so failure cannot block signals.
@app.timer_trigger(
    schedule="0 0 3 * * *",
    arg_name="statusTimer",
    run_on_startup=False,
    use_monitor=True,
)
def DailyStatusLogsTrigger(statusTimer: func.TimerRequest) -> None:
    if statusTimer.past_due:
        logging.warning("DailyStatusLogsTrigger timer is past due")
    run_pipeline("DAILY STATUS LOGS PIPELINE", status_main)


# Signals and monthly KPIs run independently 15 minutes later.
@app.timer_trigger(
    schedule="0 15 3 * * *",
    arg_name="signalsTimer",
    run_on_startup=False,
    use_monitor=True,
)
def DailySignalsTrigger(signalsTimer: func.TimerRequest) -> None:
    if signalsTimer.past_due:
        logging.warning("DailySignalsTrigger timer is past due")
    run_pipeline("DAILY SIGNALS AND MONTHLY KPI PIPELINE", signals_main)


# Dirty Dozen starts after the two source pipelines.
@app.timer_trigger(
    schedule="0 0 4 * * *",
    arg_name="dirtydozenTimer",
    run_on_startup=False,
    use_monitor=True,
)
def DailyDirtyDozenTrigger(dirtydozenTimer: func.TimerRequest) -> None:
    if dirtydozenTimer.past_due:
        logging.warning("DailyDirtyDozenTrigger timer is past due")
    run_pipeline("DAILY DIRTY DOZEN PIPELINE", dirtydozen_main)


@app.timer_trigger(
    schedule="0 0 6 8 * *",
    arg_name="appendixTimer",
    run_on_startup=False,
    use_monitor=True,
)
def MonthlyAppendixTrigger(appendixTimer: func.TimerRequest) -> None:
    if appendixTimer.past_due:
        logging.warning("MonthlyAppendixTrigger timer is past due")
    run_pipeline("MONTHLY APPENDIX PIPELINE", appendices_main)
