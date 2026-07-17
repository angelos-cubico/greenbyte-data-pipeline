import azure.functions as func
import logging

from download_status_logs_incremental import main as status_main
from download_signals_incremental import main as signals_main


app = func.FunctionApp()


@app.timer_trigger(
    schedule="0 0 3 5,15,30 * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def MonthlyReportingTrigger(myTimer: func.TimerRequest) -> None:

    if myTimer.past_due:
        logging.warning("Timer is past due")

    logging.info("=" * 80)
    logging.info("STARTING MONTHLY REPORTING PIPELINE")
    logging.info("=" * 80)

    try:
        logging.info("Running status logs pipeline...")
        status_main()

        logging.info("Status logs finished successfully.")

        logging.info("Running signals pipeline...")
        signals_main()

        logging.info("Signals pipeline finished successfully.")

        logging.info("MONTHLY REPORTING PIPELINE COMPLETED")

    except Exception as e:

        logging.exception("Monthly reporting pipeline failed")
        raise