# syntax=docker/dockerfile:1

# The image is built from the BUILD CONTEXT, not from a git clone.
#
# The previous `git clone --branch ${COURIER_REF}` meant an image built in a
# pull request contained `main`, so no CI gate on it could catch a break.
# Worse, that RUN layer's cache key is its own command text, which does not
# change as `main` advances, so a rebuild at the same ref could silently reuse
# a months-old clone.
#
# To build a PUBLISHED REF, hand docker a git URL as the context instead. The
# ref is part of the cache key, so it cannot go stale:
#
#     docker build https://github.com/CIRA-GEOIPS/courier.git#v1.0.0-alpha.36 \
#         -t courier:v1.0.0-alpha.36
#
# One ARG feeds BOTH stages so they can never drift to different Python minor
# versions. `COPY --from=builder /install /usr/local` lands in
# /usr/local/lib/python3.X/site-packages, so a drift would break `import
# courier` at runtime with no build-time error.
ARG PYTHON_IMAGE=python:3.13-alpine

# builder -- install courier and its dependencies into a relocatable prefix
FROM ${PYTHON_IMAGE} AS builder

ARG COURIER_EXTRAS=""

WORKDIR /build

# Copied by name rather than `COPY .`: the committed docs/ tree is 12 MB of a
# 13.8 MB repository, and a docs-only commit must not invalidate this layer.
# poetry-core needs exactly these (README.md is required by `readme =`).
COPY pyproject.toml README.md ./
COPY src ./src

# NON-editable, deliberately. `-e` writes a .pth naming the absolute path
# /courier/src, so the old runtime image worked only because the source tree
# was copied back to exactly that path. A regular install makes the runtime
# stage self-contained: no source tree, no path coupling, smaller, immutable.
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked \
    if [ -n "$COURIER_EXTRAS" ]; then \
      pip install --prefix=/install ".[${COURIER_EXTRAS}]"; \
    else \
      pip install --prefix=/install .; \
    fi

# runtime -- THE PUBLISHED ARTIFACT, and the default build target
FROM ${PYTHON_IMAGE} AS runtime

# bash is load-bearing, not the convenience the old comment claimed:
# courier/utils/bash_executor.py execs the hardcoded path "/bin/bash", so both
# shipped bash dispatchers break without it.
# tini reaps the /bin/bash children serial_bash and parallel_bash fork, and
# forwards SIGTERM. PID 1 does neither.
RUN apk add --no-cache bash tini \
 && adduser -D -u 1000 courier \
 && mkdir -p /work \
 && chown courier:courier /work

COPY --from=builder /install /usr/local

USER courier
WORKDIR /work

# tini ONLY -- `courier` is deliberately NOT in the ENTRYPOINT, so that the
# documented `command: ["courier", "run", ...]` compose blocks keep working
# verbatim and `docker run <image> sh` still works.
#
# The old CMD, `python -m courier`, could never work: there is no
# src/courier/__main__.py anywhere in the tree.
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["courier", "--help"]
