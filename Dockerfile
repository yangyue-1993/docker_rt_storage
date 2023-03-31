FROM fnndsc/python-poetry
EXPOSE 5555
WORKDIR /home/code
ENV WORKDIR=/home/code
COPY ./ ${WORKDIR}
RUN cd ${WORKDIR} && poetry install
ENTRYPOINT [ "/home/code/docker_rt_storage/storage.py" ]
