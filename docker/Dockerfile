FROM jupyter/scipy-notebook as serve

USER root
RUN mkdir -p /source
RUN useradd -ms /bin/bash -g root rondo-user
RUN chown rondo-user /source

COPY . /source


RUN apt-get update -y && \
  apt-get install -y build-essential && \
  apt-get install -y curl

WORKDIR /source

RUN curl -sSL https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
  && python get-pip.py && rm get-pip.py

RUN pip install virtualenv && \
    virtualenv venv && \
    source venv/bin/activate


RUN make install

USER sparkdantic-user

ENTRYPOINT ["sparkdantic"]


FROM serve as test

USER root
RUN make install-all

USER rondo-user
ENTRYPOINT ["make", "test"]
