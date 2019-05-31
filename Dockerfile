# Build from a parent image
FROM oraclelinux:7-slim as oracle

RUN  curl -o /etc/yum.repos.d/public-yum-ol7.repo https://yum.oracle.com/public-yum-ol7.repo && \
     yum-config-manager --enable ol7_oracle_instantclient && \
     yum -y install oracle-instantclient18.3-basic

FROM python:3.7

# Set the working directory
WORKDIR /desdm-dash

# Copy the current directory contents into the container
COPY . /desdm-dash

RUN groupadd -r dashuser -g 1001 &&\
    useradd -r -g dashuser -d /desdm-dash dashuser -u 1001
RUN chown -R dashuser:dashuser /desdm-dash

# Make port available to the world outside this container
EXPOSE 5000

RUN pip install --upgrade pip
RUN pip install --trusted-host pypi.python.org -r requirements.txt

RUN apt-get update
RUN apt-get -y install vim
RUN apt-get install -y supervisor
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Define environment variable
ENV NAME desdm-dash
ENV DES_SERVICES=/desdm-dash/.desservices.ini
ENV PYTHONPATH=$PYTHONPATH:/desdm-dash/app
ENV PATH=$PATH:/desdm-dash/app
ENV PATH=$PATH:/usr/lib/oracle/18.3/client64/bin
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/lib/oracle/18.3/client64/lib:/usr/lib
ENV STATIC_PATH=/desdm-dash/app/desdm-dash-static
ENV TEMPLATES_PATH=/desdm-dash/app/templates

COPY --from=oracle /usr/lib/oracle/ /usr/lib/oracle
COPY --from=oracle /lib64/libaio.so.1 /usr/lib

USER dashuser

# Run run_desdm_dash_server.sh when the container launches
CMD ["/usr/bin/supervisord"]
