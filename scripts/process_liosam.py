#!/usr/bin/env python3

from __future__ import annotations
from pathlib import Path

import click

import xbag as xb
from xbag import Session
from xbag.utils.proc import ManagedProcess
from xbag.utils import log
from xbag.pipeline.registry import register_process

from liosam_config import generate_config


@register_process("liosam")
def liosam_processor(session: Session, output_dir: Path) -> list[Path]:
    """
    LIO-SAM processor with folder-based output.

    Args:
        session: Session to process
        output_dir: Output folder (runner creates this, processor writes inside)

    Returns:
        List of created files [config.yaml, liosam_processed.bag]
    """
    if not session.has_bags:
        raise ValueError("Session has no bag files")

    # Config file inside output folder
    config_path = output_dir / "liosam_config.yaml"
    if not config_path.exists():
        raise RuntimeError(
            f"Config file not found: {config_path}\n"
            "Run 'liosam_config' processor first"
        )

    # Output bag inside folder
    output_bag = output_dir / "liosam_processed.bag"

    # Launch LIO-SAM with auto-cleanup
    liosam_cmd = [
        "roslaunch",
        "lio_sam",
        "run.launch",
        f"param_file:={config_path}",
        "enable_rviz:=false",
    ]
    with ManagedProcess(liosam_cmd, stderr=None) as liosam_proc:
        # Record LIO-SAM outputs using bag namespace
        with xb.bag.record(
            output=str(output_bag),
            topics=[
                "/lio_sam/mapping/path",
                "/odometry/imu",
                "/tf",
            ]
        ):
            # Play session with drivers using session namespace
            xb.session.play(
                session,
                rate=5.0,
                rviz=True,
                drivers=True,
            )

    return [output_bag]  # List of generated files


@click.command()
@click.argument("session_path_or_uuid", type=str)
@click.option(
    "--force",
    is_flag=True,
    help="Force re-processing even if already completed",
)
def main(session_path_or_uuid: str, force: bool):
    """LIO-SAM processing script.

    Processes a single session through LIO-SAM SLAM pipeline.

    Args:
        session_path_or_uuid: Path to session directory or session UUID
    """
    session = xb.session.open(session_path_or_uuid)

    if not session.has_bags:
        log.error(f"Session has no data: {session.info.uuid}")
        log.info("Expected structure: env_robot/session_name/raw/*.bag")
        raise click.Abort()

    # Step 1: Generate LIO-SAM configuration
    try:
        xb.pipeline.run(
            session=session,
            process_name="liosam_config",
            output_dir="liosam2",  # → processed/liosam/
            force=force,
        )
    except Exception as e:
        log.error(f"Config generation failed: {e}")
        raise click.Abort()

    # Step 2: Run LIO-SAM processor
    try:
        xb.pipeline.run(
            session=session,
            process_name="liosam",
            output_dir="liosam2",  # Same folder (config + output)
            force=force,
        )
    except KeyboardInterrupt:
        log.spacer()
        log.warning("Interrupted by user (Ctrl+C)")
        raise SystemExit(130)
    except Exception as e:
        log.error(f"LIO-SAM processing failed: {e}")
        raise click.Abort()

    log.completed("LIO-SAM processing complete!")


if __name__ == "__main__":
    main()
