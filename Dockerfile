FROM fnndsc/python-poetry
EXPOSE 5555
WORKDIR /home/code
ENV WORKDIR=/home/code
COPY ./ ${WORKDIR}
VOLUME [ "/home/code" ]
RUN cd ${WORKDIR} && poetry install