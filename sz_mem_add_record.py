#! /usr/bin/env python3

import os
import sys
import json
import time
import sqlite3
from urllib.parse import urlparse
import concurrent.futures

import itertools
BATCH_SIZE=1

from senzing import (
    SzConfig,
    SzConfigManager,
    SzEngine,
    SzEngineFlags,
    SzError,
    SzRetryableError,
    SzBadInputError,
    SzUnrecoverableError,
)

engine_config_json = os.getenv("SENZING_ENGINE_CONFIGURATION_JSON", None)
debug_trace = False

def progress(status, remaining, total):
    print(f'Copied {total-remaining} of {total} pages...')

def mock_logger(level, exception, error_rec=None):
    print(f"\n{level}: {exception}", file=sys.stderr)
    if error_rec:
        print(f"{error_rec}", file=sys.stderr)


def bulk_add_record(engine, records):
    engine.bulk_load(records)

def process_redo(engine, rec):
    engine.process_redo_record(rec)

def add_record(engine, rec_to_add):
    record_dict = json.loads(rec_to_add)
    data_source = record_dict.get("DATA_SOURCE", None)
    record_id = record_dict.get("RECORD_ID", None)
    engine.add_record(data_source, record_id, rec_to_add)


def engine_stats(engine):
    try:
        response = engine.get_stats()
        print(f"\n{response}\n")
    except SzRetryableError as err:
        mock_logger("WARN", err)
    except SzError as err:
        mock_logger("CRITICAL", err)
        raise


def record_stats(total_records, record_delta, error, prev_time):
    print(
        f"Processed {total_records:,} adds,"
        f" {int(record_delta / (time.time() - prev_time)):,} records per second,"
        f" {error} errors", flush=True
    )
    return time.time()

def futures_add(engine,file):
    prev_time = time.time()
    prev_success = success_recs = error_recs = 0

    with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {}
            for i in range(0,executor._max_workers):
                record = file.readline()
                if record:
                    futures[executor.submit(add_record, engine, record)] = (record)

            while futures:
                done, _ = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )
                for f in done:
                    try:
                        f.result()
                    except (SzBadInputError, json.JSONDecodeError) as err:
                        mock_logger("ERROR", err, futures[f])
                        error_recs += 1
                    except SzRetryableError as err:
                        mock_logger("WARN", err, futures[f])
                        error_recs += 1
                    except (SzUnrecoverableError, SzError) as err:
                        mock_logger("CRITICAL", err, futures[f])
                        raise
                    else:
                        record = file.readline()
                        if record:
                            futures[executor.submit(add_record, engine, record)] = (
                                record
                            )

                        success_recs += BATCH_SIZE
                        if success_recs % 1000 == 0:
                            #if time.time() - prev_time > 1:
                            prev_time = record_stats( success_recs, success_recs-prev_success, error_recs, prev_time)
                            prev_success = success_recs

                        if success_recs % 10000 == 0:
                            engine_stats(engine)
                        if success_recs % 100000 == 0:
                            try:
                                #cur.execute("PRAGMA optimize")
                                pass
                            except BaseException:
                                pass
                    finally:
                        del futures[f]

            print(
                f"Successfully loaded {success_recs:,} records, with"
                f" {error_recs:,} errors"
            )
            engine_stats(engine)


def futures_redo(engine):
    success_recs = error_recs = 0
    redo_paused = False

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {}
        for i in range(0,executor._max_workers):
            record = engine.get_redo_record()
            if record:
                futures[executor.submit(process_redo, engine, record)] = (record)

        while True:
            done, _ = concurrent.futures.wait(
                futures, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for f in done:
                try:
                    _ = f.result()
                    success_recs += 1

                    if success_recs % 100 == 0:
                      print(
                          f"Processed {success_recs:,} redo records, with"
                          f" {error_recs:,} errors"
                      )

                    if success_recs % 1000 == 0:
                      engine_stats(engine)
                except SzBadInputError as err:
                    mock_logger("ERROR", err, futures[f])
                    error_recs += 1
                except SzRetryableError as err:
                    mock_logger("WARN", err, futures[f])
                    error_recs += 1
                except (SzUnrecoverableError, SzError) as err:
                    mock_logger("CRITICAL", err, futures[f])
                    raise
                finally:
                    del futures[f]

            while len(futures) < executor._max_workers:
                record = engine.get_redo_record()
                if not record:
                    break
                futures[ executor.submit(process_redo, engine, record) ] = record

            if not futures:
                break


try:
    uri = json.loads(engine_config_json)["SQL"]["CONNECTION"]
    parsed = urlparse(uri)
    print(parsed)

    conn = sqlite3.connect(
            "file:" + parsed.path[1:] + "?mode=memory&cache=shared", autocommit=True
    )
    cur = conn.cursor()
    #cur.execute("pragma auto_vacuum = FULL")
    cur.execute("pragma journal_mode = OFF")
    #cur.execute("pragma SQLITE_DEFAULT_PAGE_SIZE = 8192")
    #cur.execute("pragma optimize=0x10002");
    with open("resources/schema/szcore-schema-sqlite-create.sql") as schema_file:
        for line in schema_file:
            line = line.strip()
            if not line:
                continue
            print(line)
            cur.execute(line)

    try:
        sz_config = SzConfig(
            "pySzConfig", engine_config_json, verbose_logging=debug_trace
        )
        sz_configmgr = SzConfigManager(
            "pySzConfigMgr", engine_config_json, verbose_logging=debug_trace
        )
        config_handle = sz_config.create_config()
        sz_config.add_data_source(config_handle,"ICIJ")
        default_config = sz_config.export_config(config_handle)

        # Persist new default config to Senzing Repository
        try:
            config_id = sz_configmgr.add_config(
                default_config, "New default configuration added."
            )
            sz_configmgr.set_default_config_id(config_id)
        except SzError:
            raise

        sz_engine = SzEngine(
            "pySzEngine",
            engine_config_json,
            verbose_logging=debug_trace,
        )

        #input_file = "icij_2022_100.json"
        input_file = "icij_2022.json"
        #input_file = "icij_2022_sorted-100k.json"
        with open(input_file, "r") as file:
            futures_add(sz_engine,file)
        print(f"Redo created: {sz_engine.count_redo_records()}")
        futures_redo(sz_engine)

        dst = sqlite3.connect('backup_single_add.db')
        dst_cur = dst.cursor()
        dst_cur.execute("pragma journal_mode = WAL")
        dst_cur.close()
        with dst:
            conn.backup(dst, pages=1000, progress=progress)
        dst.close()
        conn.close()

    except SzError:
        raise

except SzError as err:
    print(err)
    sys.exit(-1)
