FROM python:2

RUN apt-get update && \
    apt-get install -y python-dev rpm && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app
COPY requirements.txt 7.5.0.8-WS-MQC-LinuxX64.tar.gz ./
RUN tar -zxf 7.5.0.8-WS-MQC-LinuxX64.tar.gz && \
    ./mqlicense.sh -accept && \
    rpm --prefix /opt/mqm -ivh --nodeps --force-debian MQSeriesRuntime-7.5.0-8.x86_64.rpm && \
    rpm --prefix /opt/mqm -ivh --nodeps --force-debian MQSeriesClient-7.5.0-8.x86_64.rpm && \
    rpm --prefix /opt/mqm -ivh --nodeps --force-debian MQSeriesSDK-7.5.0-8.x86_64.rpm && \
    pip install --no-cache-dir -r requirements.txt && \
    rm -rf *.rpm *.gz PreReqs copyright crtmqpkg lap licenses mqlicense.sh repackage && \
    ln -s /opt/mqm/lib64/libmq* /usr/lib/ 

ADD uploader.py uploader.yml LICENSE ./
ADD app/ ./app/

CMD [ "python", "./uploader.py" ]
