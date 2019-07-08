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

RUN groupadd -r dashuser -g 1000 &&\
    useradd -r -g dashuser -d /desdm-dash dashuser -u 1000
RUN chown -R dashuser:dashuser /desdm-dash

# Make port available to the world outside this container
EXPOSE 5000

#CONDA
RUN apt-get -qq update && apt-get -qq -y install curl bzip2 \
    && apt-get -qq -y install vim && apt-get install -qq -y supervisor \
    && curl -sSL https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh \
    && bash /tmp/miniconda.sh -bfp /usr/local \
    && rm -rf /tmp/miniconda.sh \
    && conda install -y python=3 \
    && conda update conda \
    && apt-get -qq -y remove curl bzip2 \
    && apt-get -qq -y autoremove \
    && apt-get autoclean \
    && rm -rf /var/lib/apt/lists/* /var/log/dpkg.log \
    && conda clean --all --yes

ENV PATH /opt/conda/bin:$PATH

RUN conda install -c anaconda oracle-instantclient
RUN conda install easyaccess -c mgckind 
RUN conda install -y pip 

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

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
