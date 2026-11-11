# Test Kafka broker with SASL_SSL (SASL PLAIN + TLS with client certificate).
# TEST ONLY: the credentials and certificates in this folder are public.
FROM apache/kafka:4.0.0

COPY --chown=appuser:appuser server.properties start.sh /opt/kafka/config/sasl-ssl/
COPY --chown=appuser:appuser secrets/ /opt/kafka/config/sasl-ssl/secrets/

# Any fixed 22-character base64 UUID works for a single test node
ENV CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk

EXPOSE 9092

CMD ["bash", "/opt/kafka/config/sasl-ssl/start.sh"]
