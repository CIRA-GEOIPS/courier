# Seperate builder stage so the clone doesn't travel with the built code
FROM python:3.13-alpine AS builder

RUN apk add --no-cache git

ARG COURIER_EXTRAS=""
ARG COURIER_REF=main

RUN git clone --depth 1 --branch ${COURIER_REF} \
    https://github.com/CIRA-GEOIPS/courier.git /courier

WORKDIR /courier

# Install into a specific location for copying into the runtime stage
RUN if [ -n "$COURIER_EXTRAS" ]; then \
      pip install --no-cache-dir --prefix=/install -e ".[${COURIER_EXTRAS}]"; \
    else \
      pip install --no-cache-dir --prefix=/install -e .; \
    fi

# Runtime stage
FROM python:3.13-alpine

 # install bash for ease of use over ash
RUN apk add --no-cache bash

COPY --from=builder /install /usr/local
COPY --from=builder /courier /courier
WORKDIR /courier

CMD ["python", "-m", "courier"]
