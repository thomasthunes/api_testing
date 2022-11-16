FROM centos:7 as base

ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

# Install required packages
RUN yum -y install epel-release
RUN yum -y install gcc\
                   git\
                   libsodium\
                   openssl\
                   python3-devel\
                   python3-pip\
                   sudo
RUN pip3 install pip --upgrade

##########################################################################################
FROM base as run

ARG user=appuser
ARG group=appuser
ARG uid=1000
ARG gid=1000

RUN groupadd -g ${gid} ${group} && \
    useradd -u ${uid} -g ${group} -s /bin/sh ${user}

WORKDIR /app

# Install dependencies
COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . ./tsd-file-api
# Fix permission to access files and directories for non root users
RUN find . -type d -exec chmod 755 {} +
RUN find . -type f -exec chmod 644 {} +
RUN cd ./tsd-file-api && pip3 install  . -t /usr/local/lib/python3.6/site-packages/
RUN cp    ./tsd-file-api/scripts/generic-chowner /usr/local/bin/chowner

# Testing
RUN mkdir -p /data/p11/
RUN chown ${user}:${group} -R /data/
RUN chmod g+rwx -R /data/

RUN mkdir /.gnupg
RUN chmod g+rwx /.gnupg

# Switch user
USER ${user}

EXPOSE 3003

ENTRYPOINT python3  /usr/local/lib/python3.6/site-packages/tsdfileapi/api.py  "$TSD_FILE_API_CONFIG"
# To test:
#  python3  /usr/local/lib/python3.6/site-packages/tsdfileapi/test_file_api.py "$TSD_FILE_API_CONFIG" all

##########################################################################################
FROM base as dev

RUN pip3 install flake8 pylint

ADD . ./tsd-file-api

RUN cd ./tsd-file-api  &&  pip3 install --no-cache-dir -r requirements.txt
RUN cd ./tsd-file-api && python3 setup.py develop

# Testing
RUN mkdir -p /tmp/p11/survey
RUN mkdir -p /tmp/p11/cluster
RUN mkdir -p /tmp/p12/
RUN cp    ./tsd-file-api/scripts/generic-chowner /usr/local/bin/chowner
RUN cp -r ./tsd-file-api/tsdfileapi/data/tsd/p11/export/   /tmp/p11/.
RUN groupadd  p11-member-group && useradd -g p11-member-group p11-nobody
RUN groupadd  p12-member-group && adduser -g p12-member-group p12

EXPOSE 3003


ENTRYPOINT python3 ~/tsd-file-api/tsdfileapi/api.py
# ENTRYPOINT sleep 10000000
# CMD  /etc/tsd/tsd-file-api/config.yaml
# python3 ~/tsd-file-api/tsdfileapi/api.py /etc/tsd/tsd-file-api/config.yaml
# python3 ~/tsd-file-api/tsdfileapi/test_file_api.py /etc/tsd/tsd-file-api/config-test.yaml  all

##########################################################################################
FROM base as rpm

RUN yum -y install make\
                   rpm-build
# Workaround to get ruby2.3:
# ERROR:  Error installing fpm:
#       ffi requires Ruby version >= 2.3.
RUN yum -y install centos-release-scl-rh centos-release-scl
RUN yum --enablerepo=centos-sclo-rh -y install rh-ruby23 rh-ruby23-ruby-devel
RUN source /opt/rh/rh-ruby23/enable && gem install --no-ri --no-rdoc fpm

RUN pip3 install virtualenv virtualenv-tools3

# build rpms
WORKDIR /file-api
COPY requirements.txt ./
RUN mkdir -p dist

RUN source /opt/rh/rh-ruby23/enable &&\
    fpm --verbose -s virtualenv -p /file-api/dist\
    -t rpm --name tsd-file-api-venv --version 2.16\
    --prefix /opt/tsd-file-api-venv/virtualenv requirements.txt

COPY . ./

RUN python3 setup.py bdist --format=rpm
