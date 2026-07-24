import azure.functions as func
import logging

from download_status_logs_incremental import main as status_main
from download_signals_incremental import main as signals_main
from generate_appendices_common_graph import main as appendices_main

app = func.FunctionApp()


# ------------------------------------------------------------------
# DAILY GREENBYTE -> BLOB PIPELINE
# Runs every day at 03:00 UTC
# ------------------------------------------------------------------
@app.timer_trigger(
    schedule="0 0 3 * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def DailyReportingDataPullTrigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.warning("DailyReportingDataPullTrigger timer is past due")

    logging.warning("=" * 80)
    logging.warning("DAILY GREENBYTE TO BLOB PIPELINE START")
    logging.warning("=" * 80)

    try:
        logging.warning("STATUS PIPELINE START")
        status_main()
        logging.warning("STATUS PIPELINE END")

        logging.warning("SIGNALS PIPELINE START")
        signals_main()
        logging.warning("SIGNALS PIPELINE END")

        logging.warning("=" * 80)
        logging.warning("DAILY GREENBYTE TO BLOB PIPELINE COMPLETE")
        logging.warning("=" * 80)

    except Exception:
        logging.exception("DAILY GREENBYTE TO BLOB PIPELINE FAILED")
        raise


# ------------------------------------------------------------------
# MONTHLY PDF APPENDIX GENERATION
# Runs on the 8th day of every month at 06:00 UTC
# ------------------------------------------------------------------
@app.timer_trigger(
    schedule="0 0 6 8 * *",
    arg_name="appendixTimer",
    run_on_startup=False,
    use_monitor=False,
)
def MonthlyAppendixTrigger(appendixTimer: func.TimerRequest) -> None:
    if appendixTimer.past_due:
        logging.warning("MonthlyAppendixTrigger timer is past due")

    logging.warning("=" * 80)
    logging.warning("MONTHLY APPENDIX PIPELINE START")
    logging.warning("=" * 80)

    try:
        appendices_main()

        logging.warning("=" * 80)
        logging.warning("MONTHLY APPENDIX PIPELINE COMPLETE")
        logging.warning("=" * 80)

    except Exception:
        logging.exception("MONTHLY APPENDIX PIPELINE FAILED")
        raise
