FROM ubuntu:14.04
MAINTAINER Alex Baden / Neurodata (neurodata.io)

RUN apt-get clean 
RUN apt-get update
RUN apt-get -y upgrade 

RUN apt-get -y install build-essential

RUN apt-get -y install \
  python-pip \
  python-all-dev \
  zlib1g-dev \
  libjpeg8-dev \
  libtiff4-dev \
  libfreetype6-dev \
  liblcms2-dev \
  libwebp-dev \
  tcl8.5-dev \ 
  tk8.5-dev \ 
  python-tk \
  libhdf5-dev \
  git vim 

# install numpy
RUN pip install numpy 

# install ndio
RUN pip install ndio


