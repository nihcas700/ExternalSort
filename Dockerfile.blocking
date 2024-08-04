FROM openjdk:latest
COPY ./build/libs/Sorting-1.0-SNAPSHOT.jar /usr/app/
WORKDIR /usr/app
RUN sh -c 'touch Sorting-1.0-SNAPSHOT.jar'
ENTRYPOINT ["java", "-Xmx4096M", "-Dcom.sun.management.jmxremote=true", "-Dcom.sun.management.jmxremote.port=5005", "-Dcom.sun.management.jmxremote.local.only=false", "-Dcom.sun.management.jmxremote.authenticate=false", "-Dcom.sun.management.jmxremote.ssl=false", "-Dcom.sun.management.jmxremote.rmi.port=5005", "-Djava.rmi.server.hostname=localhost", "-jar","Sorting-1.0-SNAPSHOT.jar", "createInput", "clearIntermediate", "runBlockingIO"]
