# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

ARG FROM=centos:8
FROM ${FROM}

COPY qemu-* /usr/bin/

ARG DEBUG

RUN \
  quiet=$([ "${DEBUG}" = "yes" ] || echo "--quiet") && \
  # CentOS 8.0.1905 provides LLVM 7 but CentOS 8.1.1911 provides only
  # LLVM 8. We should remove this when we support LLVM 8.
  sed -i'' \
    -e 's/^mirrorlist/#mirrorlist/' \
    -e 's/^#baseurl/baseurl/' \
    -e 's/$releasever/8.0.1905/g' \
    /etc/yum.repos.d/*.repo && \
  dnf install -y ${quiet} epel-release && \
  dnf install --enablerepo=PowerTools -y ${quiet} \
    bison \
    boost-devel \
    brotli-devel \
    bzip2-devel \
    clang \
    cmake \
    flex \
    gcc-c++ \
    gflags-devel \
    git \
    glog-devel \
    gobject-introspection-devel \
    gtk-doc \
    libzstd-devel \
    llvm-devel \
    llvm-static \
    lz4-devel \
    make \
    ncurses-devel \
    ninja-build \
    openssl-devel \
    pkg-config \
    python3 \
    python3-devel \
    python3-numpy \
    python3-pip \
    re2-devel \
    # rapidjson-devel \
    rpm-build \
    rpmdevtools \
    snappy-devel \
    tar \
    zlib-devel && \
  dnf clean ${quiet} all
