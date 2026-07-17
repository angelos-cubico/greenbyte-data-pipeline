import azure.functions as func
import logging

from download_status_logs_incremental import main as status_main
from download_signals_incremental import main as signals_main

app = func.FunctionApp()


@app.timer_trigger(
    schedule="0 */2 * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def MonthlyReportingTrigger(myTimer: func.TimerRequest) -> None:

    if myTimer.past_due:
        logging.warning("Timer is past due")

    logging.warning("=" * 80)
    logging.warning("MONTHLY REPORTING PIPELINE START")
    logging.warning("=" * 80)

    try:

        logging.warning("STATUS PIPELINE START")
        status_main()
        logging.warning("STATUS PIPELINE END")

        logging.warning("SIGNALS PIPELINE START")
        signals_main()
        logging.warning("SIGNALS PIPELINE END")

        logging.warning("=" * 80)
        logging.warning("MONTHLY REPORTING PIPELINE COMPLETE")
        logging.warning("=" * 80)

    except Exception:

        logging.exception("MONTHLY REPORTING PIPELINE FAILED")

        raise