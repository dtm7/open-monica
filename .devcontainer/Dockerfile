FROM mcr.microsoft.com/devcontainers/base:bookworm

RUN export DEBIAN_FRONTEND=noninteractive; \
    apt-get update \
    && echo "deb-src  http://deb.debian.org/debian bookworm main" > /etc/apt/sources.list.d/debian-src.list \
    && echo "deb [arch=amd64] http://nexus-repo.atnf.csiro.au/repository/askapsoft-tos-bookworm/ bookworm main" \
        > /etc/apt/sources.list.d/tos-bookworm.list \
    && apt-get install -y wget sudo \
    && apt-get install -y ant ant-optional junit4 zeroc-ice-compilers libzeroc-ice3.7-java openjdk-17-jdk curl \
    && wget -qO- http://nexus-repo.atnf.csiro.au/repository/hosted-gpg-keys/nexus-oss-public.gpg.key | \
        tee /etc/apt/trusted.gpg.d/nexus-oss-public.asc \
    && apt-get update && apt-get install -y devscripts

RUN echo "mk-build-deps --remove --install debian/control -t 'apt-get -y -o Debug::pkgProblemResolver=yes --no-install-recommends'" > /usr/local/bin/install-deps
RUN chmod a+x /usr/local/bin/install-deps
