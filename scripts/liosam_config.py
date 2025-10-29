from __future__ import annotations

import yaml
from pathlib import Path
from typing import Any

from xbag.session import Session
from xbag.pipeline.registry import register_process
from xbag.utils import log


BASE_CONFIG_PATH = Path(__file__).parent.parent / "config" / "params.yaml"
LIDAR_PRESET_PATH = Path(__file__).parent.parent / "config" / "lidar_presets.yaml"


def load_base_config() -> dict[str, Any]:
    if not BASE_CONFIG_PATH.exists():
        raise FileNotFoundError(f"Base config not found: {BASE_CONFIG_PATH}")

    with open(BASE_CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def load_lidar_preset(vendor: str) -> dict[str, Any]:
    if not LIDAR_PRESET_PATH.exists():
        raise FileNotFoundError(f"Preset file not found: {LIDAR_PRESET_PATH}")

    with open(LIDAR_PRESET_PATH, "r") as f:
        presets = yaml.safe_load(f)

    if vendor not in presets:
        available = ", ".join(presets.keys())
        raise ValueError(
            f"No preset found for vendor '{vendor}'\n" f"Available vendors: {available}"
        )
    return presets[vendor]


@register_process("liosam_config")
def generate_config(session: Session, output_dir: Path) -> list[Path]:
    """
    Generate LIO-SAM configuration from session sensors and calibration.

    Returns:
        List of created files
    """
    from xbag.transforms.tf_tree import TfTree

    # Get sensors: prefer velodyne LiDAR if available
    lidars_3d = session.sensors.lidars_3d
    if not lidars_3d:
        raise RuntimeError("No 3D LiDAR sensor found in session")

    # Prefer velodyne, otherwise use first (highest priority)
    lidar = None
    for l in lidars_3d:
        if "velodyne" in l.name.lower():
            lidar = l
            break
    if lidar is None:
        lidar = lidars_3d[0]

    # Get IMU
    imus = session.sensors.imus
    if not imus:
        raise RuntimeError("No IMU sensor found in session")
    imu = imus[0]

    log.info(f"LiDAR: {lidar.name} ({lidar.frame_id})", indent=2)
    log.info(f"IMU: {imu.name} ({imu.frame_id})", indent=2)

    # Load TF tree from calibration
    calib_file = session.files.get_calibration("default.yaml")
    if not calib_file or not calib_file.exists():
        raise RuntimeError(
            f"Calibration file not found: {session.dirs.calibration}/default.yaml"
        )

    # Get transform from LiDAR to IMU using TF tree
    tf_tree = TfTree.from_file(calib_file)
    if not tf_tree or not tf_tree.has_transform:
        raise RuntimeError(f"No transforms found in calibration file: {calib_file}")

    transform = tf_tree.get_transform(lidar.frame_id, imu.frame_id)
    if transform is None:
        available_frames = tf_tree.get_all_frames()
        raise RuntimeError(
            f"Transform not found: {lidar.frame_id} → {imu.frame_id}\n"
            f"Available frames: {available_frames}"
        )

    log.info(
        f"Transform: {lidar.frame_id} → {imu.frame_id}",
        indent=2,
    )

    # Extract translation and rotation for LIO-SAM
    extrinsic_trans = transform.translation.as_list()  # [tx, ty, tz]
    extrinsic_rot = [
        elem for row in transform.rotation.as_list() for elem in row
    ]  # Flatten to [r00, r01, ..., r22]

    # Detect vendor from lidar name
    vendor = None
    for v in ["velodyne", "ouster"]:
        if v in lidar.name.lower():
            vendor = v
            break
    if not vendor:
        raise RuntimeError(f"Unknown LiDAR vendor in name: {lidar.name}")

    # Load sensor preset
    preset = load_lidar_preset(vendor)
    log.info(
        f"Sensor preset: {vendor} "
        f"(N_SCAN={preset['N_SCAN']}, Horizon_SCAN={preset['Horizon_SCAN']})",
        indent=2,
    )

    # Get topics using sensor properties
    cloud_topic = lidar.cloud_topic
    if not cloud_topic:
        raise RuntimeError(f"No point cloud topic found for LiDAR: {lidar.name}")

    imu_topic = imu.imu_topic
    if not imu_topic:
        raise RuntimeError(f"No IMU topic found for IMU: {imu.name}")

    log.info(f"Point cloud topic: {cloud_topic}", indent=2)
    log.info(f"IMU topic: {imu_topic}", indent=2)

    # Load and update config
    config = load_base_config()
    if "lio_sam" not in config:
        config["lio_sam"] = {}
    config["lio_sam"].update(
        {
            "pointCloudTopic": cloud_topic,
            "imuTopic": imu_topic,
            "lidarFrame": lidar.frame_id,
            "baselinkFrame": "base_link",  # Standard ROS convention
            "sensor": preset["sensor"],
            "N_SCAN": preset["N_SCAN"],
            "Horizon_SCAN": preset["Horizon_SCAN"],
            "extrinsicTrans": extrinsic_trans,
            "extrinsicRot": extrinsic_rot,
            "extrinsicRPY": extrinsic_rot,  # LIO-SAM uses rotation matrix for both
        }
    )

    # Save to output folder
    output_file = output_dir / "liosam_config.yaml"
    with open(output_file, "w") as f:
        yaml.safe_dump(config, f, sort_keys=False)

    log.completed(f"Config generated: {output_file.name}", indent=2)

    return [output_file]
