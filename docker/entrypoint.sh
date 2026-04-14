#!/bin/bash
# LIO-SAM entrypoint for xbag runner.
# Expects: --network=host (shares host ROS master + xbag play topics)
# Mounts:  /config/params.yaml (rendered config), /output (pose.tum + record bag)

source /opt/ros/noetic/setup.bash
source /ws/devel/setup.bash

# 1. Load config
rosparam load /config/params.yaml

# 2. Launch LIO-SAM nodes (background)
roslaunch lio_sam module_loam.launch --wait &
LAUNCH_PID=$!
sleep 3

# 3. Record output topics (background)
rosbag record -O /output/record \
  /lio_sam/mapping/cloud_deskewed_body &
PID_REC=$!

# 4. Shutdown handler
SHUTDOWN=0
shutdown() {
    SHUTDOWN=1

    # Stop recording first — SIGINT flushes bag header
    kill -INT $PID_REC 2>/dev/null
    wait $PID_REC 2>/dev/null || true

    # Stop roslaunch — mapOptmization saves /output/pose.tum on SIGINT
    kill -INT $LAUNCH_PID 2>/dev/null
    wait $LAUNCH_PID 2>/dev/null || true
}
trap shutdown INT TERM

# 5. Wait — loop to ensure trap is handled
while [ $SHUTDOWN -eq 0 ]; do
    wait -n 2>/dev/null || true
done
