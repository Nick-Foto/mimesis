FROM ubuntu

WORKDIR /app

RUN apt-get update
RUN apt-get install python3 -y
RUN apt-get install python3-pip -y
COPY anonpeoplecsv.py makepeoplecsv.py requirements.txt Dockerfile  ./

RUN pip3 install -r requirements.txt


CMD ["bash"]