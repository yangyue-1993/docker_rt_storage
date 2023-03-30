FROM fnndsc/python-poetry
EXPOSE 5555
WORKDIR /home/code
ENV WORKDIR=/home/code
COPY ./ ${WORKDIR}
RUN cd ${WORKDIR} && poetry install