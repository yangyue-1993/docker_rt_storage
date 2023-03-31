FROM fnndsc/python-poetry
EXPOSE 5555
WORKDIR /home/code
ENV WORKDIR=/home/code
COPY ./ ${WORKDIR}
RUN cd ${WORKDIR} && poetry install
RUN chmod a+x /home/code/docker_rt_storage/storage.py
ENTRYPOINT [ "python", "/home/code/docker_rt_storage/storage.py" ]
