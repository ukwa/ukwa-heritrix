FROM ukwa/crawl-streams

COPY requirements.txt /tmp/requirements.txt
RUN  pip install -r /tmp/requirements.txt

COPY tests /tests

ENTRYPOINT [ "robot" ]

