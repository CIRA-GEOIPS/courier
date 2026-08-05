FROM python:3.13-alpine

# install dependencies
RUN apk add \
  git 

# clone courier and install courier dependencies
RUN git clone https://github.com/CIRA-GEOIPS/courier.git
WORKDIR /courier

RUN python -m venv .venv && source .venv/bin/activate

# pyproject.toml args
ARG EXTRAS=""

RUN if [ -n "$EXTRAS" ]; then \
    pip install -e ".[${EXTRAS}]"; \
  else \
    pip install -e .; \
  fi
