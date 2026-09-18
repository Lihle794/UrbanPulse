FROM apache/spark:4.0.4-python3

USER root

ENV HOME=/tmp

WORKDIR /opt/urbanpulse

# Bake UrbanPulse processing code into the execution image.
COPY --chown=185:0 processing/ /opt/urbanpulse/processing/

USER 185