FROM ros:noetic-ros-base

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    libeigen3-dev \
    libpcl-dev \
    libboost-all-dev \
    libopencv-dev \
    ros-noetic-pcl-ros \
    ros-noetic-pcl-conversions \
    ros-noetic-tf \
    ros-noetic-cv-bridge \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

# GTSAM
RUN add-apt-repository ppa:borglab/gtsam-release-4.0 \
    && apt-get update \
    && apt-get install -y --no-install-recommends libgtsam-dev libgtsam-unstable-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy source and build
WORKDIR /ws/src
COPY . /ws/src/lio_sam

WORKDIR /ws
RUN /bin/bash -c "source /opt/ros/noetic/setup.bash && \
    catkin_make -DCMAKE_BUILD_TYPE=Release -j$(nproc)"

COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
