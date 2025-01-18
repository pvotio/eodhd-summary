from client.engine import Engine
from config import logger
from database.helper import init_db_instance, load_tickers
from transformer import Agent


def main():
    logger.info("Loading tickers...")
    tickers = load_tickers()[:100]
    logger.info(f"{len(tickers)} Tickers loaded from database.")
    logger.info("Initializing Engine...")
    engine = Engine(tickers)
    logger.info("Engine initialized successfully.")
    logger.info("Running Engine...")
    engine.run()
    logger.info("Engine run completed successfully.")

    logger.info("Transforming data...")
    transformer = Agent(engine.data)
    tables = transformer.transform()

    logger.info("Saving data to database...")
    conn = init_db_instance()
    for t, dataframe in tables.items():
        if not dataframe.empty:
            logger.info(f"\n{dataframe}")
            conn.insert_table(dataframe, t)

    logger.info("Process completed successfully")


if __name__ == "__main__":
    main()
