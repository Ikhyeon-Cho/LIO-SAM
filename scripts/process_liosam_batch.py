#!/usr/bin/env python3
"""
Batch LIO-SAM processing script

Usage:
    python process_liosam_batch.py --robot husky --date 2024-01 --force
"""

from __future__ import annotations

import click
import xbag as xb
from xbag.utils import log

from liosam_config import generate_config
from process_liosam import liosam_processor


@click.command()
@click.option("--dataset", help="Dataset name from config")
@click.option("--robot", help="Filter by robot name")
@click.option("--env", help="Filter by environment name")
@click.option("--date", help="Filter by date (YYYY-MM-DD or YYYY-MM)")
@click.option("--force", is_flag=True, help="Force re-processing")
def main(
    dataset: str | None,
    robot: str | None,
    env: str | None,
    date: str | None,
    force: bool,
):
    """Batch LIO-SAM processing script.

    Processes multiple sessions through LIO-SAM SLAM pipeline with filtering options.
    """
    # Load and filter dataset
    ds = xb.dataset.from_config(dataset) if dataset else xb.dataset.from_config()
    filtered = ds.filter(robot=robot, env=env, date=date)

    if not filtered:
        log.warning("No sessions found")
        return

    # Show plan
    filter_info = [
        robot and f"Robot: {robot}",
        env and f"Environment: {env}",
        date and f"Date: {date}",
        f"Force: {force}",
    ]
    log.header(f"LIO-SAM Batch Processing ({len(filtered)} sessions)")
    for info in filter(None, filter_info):
        log.info(info, indent=1)
    print()

    # Process sessions
    success_count = fail_count = 0

    try:
        for idx, session in enumerate(filtered, 1):
            log.separator()
            log.info(f"[{idx}/{len(filtered)}] {session.info.uuid}")
            print()

            # Config generation
            try:
                xb.pipeline.run(
                    session=session,
                    process_name="liosam_config",
                    output_dir="liosam",
                    force=force,
                )
            except Exception as e:
                log.error(f"Config generation failed: {e}")
                fail_count += 1
                continue

            # LIO-SAM processing
            try:
                xb.pipeline.run(
                    session=session,
                    process_name="liosam",
                    output_dir="liosam",
                    force=force,
                )
                success_count += 1
            except Exception as e:
                log.error(f"LIO-SAM processing failed: {e}")
                fail_count += 1

    except KeyboardInterrupt:
        log.spacer()
        log.warning("Interrupted by user (Ctrl+C)")

    # Summary
    log.spacer()
    log.separator()
    log.header("Summary")
    log.info(f"Total: {len(filtered)}", indent=1)
    log.success(f"Completed: {success_count}", indent=1)
    if fail_count:
        log.error(f"Failed: {fail_count}", indent=1)

    if fail_count:
        raise click.Abort()


if __name__ == "__main__":
    main()
