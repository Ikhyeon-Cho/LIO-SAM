#!/bin/bash
# LIO-SAM entrypoint for xbag runner.
# Expects: --network=host (shares host ROS master + xbag play topics)
# Mounts:  /config/params.yaml (rendered config), /output (pose.tum + record bag)

source /opt/ros/noetic/setup.bash
source /ws/devel/setup.bash

# 1. Load config (override the default params.yaml)
rosparam load /config/params.yaml

# 2. Launch LIO-SAM nodes (background) — launch file assigns unique node names
roslaunch lio_sam module_loam.launch --wait &
LAUNCH_PID=$!
sleep 3

# 3. Record output topics (background)
rosbag record -O /output/record \
  /lio_sam/mapping/cloud_deskewed_body &
PID_REC=$!

# 4. Handle SIGINT: stop record first (flush), then algorithm
shutdown() {
    # Stop recording — SIGINT flushes bag header
    kill -INT $PID_REC 2>/dev/null
    wait $PID_REC 2>/dev/null || true

    # Stop roslaunch — propagates SIGINT to all nodes
    # mapOptmization's signal handler saves /output/pose.tum
    kill -INT $LAUNCH_PID 2>/dev/null
    wait $LAUNCH_PID 2>/dev/null || true

    exit 0
}
trap shutdown INT TERM

# 5. Wait indefinitely — host sends SIGINT after xbag play finishes
wait
