FROM ukwa/crawl-streams:master

COPY requirements.txt /tmp/requirements.txt
RUN  pip install -r /tmp/requirements.txt

COPY tests /tests

ENTRYPOINT [ "robot", "--exitonfailure" ]

