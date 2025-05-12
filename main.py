from client.engine import Engine
from config import logger
from database.helper import init_db_instance, load_tickers
from transformer import Agent
from config.settings import INSERT_CHUNK_COUNT, CLIENT_BATCH_SIZE


def create_batches(tickers):
    batches = []
    step = CLIENT_BATCH_SIZE
    for z in range(0, len(tickers), step):
        batches.append(tickers[z : z + step])

    logger.debug(
        f"Tickers split into {len(batches)} batch(es) with up to {step} tickers each."
    )
    return batches


def calculate_chunk_size(df):
    chunk_count = INSERT_CHUNK_COUNT
    if len(df) <= chunk_count:
        logger.debug(
            f"DataFrame has {len(df)} rows, using chunk size equal to row count."
        )
        return len(df)

    chunk_size = int(len(df) / chunk_count)
    logger.debug(f"DataFrame has {len(df)} rows, calculated chunk size: {chunk_size}.")
    return chunk_size


def main():
    insertion_state = {}
    logger.info("Starting data processing pipeline...")

    logger.info("Loading tickers from database...")
    tickers = load_tickers()
    logger.info(f"{len(tickers)} tickers loaded.")

    batches = create_batches(tickers)

    for i, batch in enumerate(batches):
        logger.info(
            f"\n=== Processing Batch #{i+1} of {len(batches)} (Size: {len(batch)}) ==="
        )

        logger.info("Initializing Engine...")
        engine = Engine(batch)
        logger.info("Engine initialized.")

        logger.info("Running Engine to fetch data...")
        engine.run()
        logger.info("Engine run completed. Data fetched.")

        logger.info("Transforming fetched data using Agent...")
        transformer = Agent(engine.data)
        tables = transformer.transform()
        logger.info(f"Transformation complete.")

        logger.info("Establishing database connection...")
        conn = init_db_instance()

        for t, dataframe in tables.items():
            logger.info(f"\nProcessing table '{t}' with {len(dataframe)} row(s)...")

            delete_prev_records = t not in insertion_state
            if delete_prev_records:
                insertion_state[t] = True

            if not dataframe.empty:
                logger.debug(f"Data preview for table '{t}':\n{dataframe.head()}\n...")
                chunk_size = calculate_chunk_size(dataframe)
                logger.debug(
                    f"Inserting data into table '{t}' with chunk size {chunk_size}..."
                )
                conn.insert_table(
                    dataframe,
                    t,
                    delete_prev_records=delete_prev_records,
                    chunk_size=chunk_size,
                )
                logger.info(f"Data inserted into table '{t}' successfully.")
            else:
                logger.warning(f"No data to insert for table '{t}'. Skipping.")

    logger.info("\nPipeline execution completed.")


if __name__ == "__main__":
    main()
