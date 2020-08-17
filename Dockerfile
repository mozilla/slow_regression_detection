FROM continuumio/anaconda3
# RUN mkdir /tmp/output/

# RUN R -e "options(repos = list(CRAN = 'https://cran.microsoft.com/snapshot/2020-04-10/')); \
#           pkgs <- c('vegawidget','parsedate','logging', 'Hmisc', 'ggplot2','glue','DBI','bigrquery','gargle','data.table','knitr','rmarkdown'); \
#           install.packages(pkgs,dep=TRUE);"

RUN apt-get update && apt-get install -y \
        bzr \
        gnupg2 \
        cvs \
        git \
        curl \
        mercurial \
        subversion

# install google cloud sdk
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
RUN apt-get update && apt-get install -y google-cloud-sdk

# clean up now-unnecessary packages + apt-get cruft
RUN apt-get remove -y gnupg curl 
RUN apt-get autoremove -y && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN conda update conda
RUN conda update anaconda

# RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && apt-get update -y && apt-get install google-cloud-sdk -y

COPY env_r.yaml /tmp/
RUN conda env update -n base -f /tmp/env_r.yaml

COPY requirements_dev.txt /tmp/
# RUN pip install -r /tmp/requirements_dev.txt

WORKDIR /sreg

# RUN echo "project_id = moz-fx-data-shared-prod" > /root/.bigqueryrc

CMD /bin/bash etl.sh
