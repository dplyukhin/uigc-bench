FROM sbtscala/scala-sbt:eclipse-temurin-jammy-17.0.10_7_1.10.2_2.13.15

# Install git, curl, and python3
RUN apt-get update
RUN apt-get install -y git python3 python3-pip

# Install pandas, pyplot, and numpy
RUN pip3 install pandas matplotlib numpy

# Build Pekko/UIGC
RUN git clone --depth 1 https://github.com/dplyukhin/uigc-pekko.git ~/uigc-pekko
WORKDIR ~/uigc-pekko
RUN sbt publishLocal